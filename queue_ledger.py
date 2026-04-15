# ==========================================================
# [queue_ledger.py]
# ⚠️ 신규 역추세 엔진(V_REV) 전용 LIFO 로트(Lot) 장부 관리 모듈
# 💡 [핵심 수술] 수량 동기화(CALIB) 및 Pop 차감 로직 내 Safe Casting (None 방어) 전면 이식 완료
# 🚨 [V27.02 핫픽스] 동일 일자(Same Day) 로트(Lot) 파편화 방지 및 자동 병합(Merge) 엔진 탑재
# 🚨 [V27.02 핫픽스] CALIB_ADD (보정 추가) 시 평단가 $0.00 붕괴 버그 원천 차단
# ==========================================================
import os
import json
import time
from datetime import datetime

class QueueLedger:
    def __init__(self, file_path="data/queue_ledger.json"):
        self.file_path = file_path
        self._ensure_file()

    def _ensure_file(self):
        dir_name = os.path.dirname(self.file_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        if not os.path.exists(self.file_path):
            self._save({})

    def _load(self):
        for _ in range(3):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                time.sleep(0.1)
        return {}

    def _save(self, data):
        for _ in range(3):
            try:
                with open(self.file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                return
            except Exception:
                time.sleep(0.1)

    def get_queue(self, ticker):
        data = self._load()
        return data.get(ticker, [])

    def get_total_qty(self, ticker):
        q = self.get_queue(ticker)
        return sum(int(float(item.get("qty") or 0)) for item in q)

    def add_lot(self, ticker, qty, price, lot_type="NORMAL"):
        qty = int(float(qty or 0))
        if qty <= 0: return
        
        data = self._load()
        q = data.get(ticker, [])
        
        # NEW: [V27.02] 동일 일자 병합 로직 (파편화 방지)
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if q and q[-1].get("date", "").startswith(today_str):
            old_qty = int(float(q[-1].get("qty", 0)))
            old_price = float(q[-1].get("price", 0.0))
            
            new_qty = old_qty + qty
            # 평단가 가중 평균 재계산
            new_price = ((old_qty * old_price) + (qty * float(price))) / new_qty if new_qty > 0 else 0.0
            
            q[-1]["qty"] = new_qty
            q[-1]["price"] = round(new_price, 4)
            q[-1]["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            q.append({
                "qty": qty,
                "price": float(price or 0.0),
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": lot_type
            })
            
        data[ticker] = q
        self._save(data)

    def pop_lots(self, ticker, target_qty):
        target_qty = int(float(target_qty or 0))
        if target_qty <= 0: return 0
        data = self._load()
        q = data.get(ticker, [])
        popped_total = 0

        while q and target_qty > 0:
            last_lot = q[-1]
            lot_qty = int(float(last_lot.get("qty") or 0))
            if lot_qty <= target_qty:
                popped = q.pop()
                popped_qty = int(float(popped.get("qty") or 0))
                popped_total += popped_qty
                target_qty -= popped_qty
            else:
                last_lot["qty"] = lot_qty - target_qty
                popped_total += target_qty
                target_qty = 0

        data[ticker] = q
        self._save(data)
        return popped_total

    # MODIFIED: [V27.02] actual_avg 파라미터 추가 및 0.0 달러 평단가 붕괴 방어
    def sync_with_broker(self, ticker, actual_qty, actual_avg=0.0):
        data = self._load()
        q = data.get(ticker, [])
        current_q_qty = sum(int(float(item.get("qty") or 0)) for item in q)
        actual_qty = int(float(actual_qty or 0))

        if current_q_qty == actual_qty:
            return False 

        today_str = datetime.now().strftime("%Y-%m-%d")

        if current_q_qty < actual_qty:
            diff = actual_qty - current_q_qty
            
            # 💡 [핵심 수술] 평단가 0.0달러 방어
            calib_price = float(actual_avg)
            if calib_price <= 0.0:
                # actual_avg가 0.0으로 넘어오면 큐의 마지막 평단가를 복사 (최소한 0.0은 막음)
                calib_price = float(q[-1].get("price", 0.0)) if q else 0.0
            
            # 💡 [핵심 수술] 동일 일자 병합
            if q and q[-1].get("date", "").startswith(today_str):
                old_qty = int(float(q[-1].get("qty", 0)))
                old_price = float(q[-1].get("price", 0.0))
                
                new_qty = old_qty + diff
                new_price = ((old_qty * old_price) + (diff * calib_price)) / new_qty if new_qty > 0 else 0.0
                
                q[-1]["qty"] = new_qty
                q[-1]["price"] = round(new_price, 4)
                q[-1]["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                q.append({
                    "qty": diff,
                    "price": round(calib_price, 4), 
                    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "type": "CALIB_ADD"
                })
        else:
            diff = current_q_qty - actual_qty
            while q and diff > 0:
                last_lot = q[-1]
                lot_qty = int(float(last_lot.get("qty") or 0))
                if lot_qty <= diff:
                    popped = q.pop()
                    diff -= int(float(popped.get("qty") or 0))
                else:
                    last_lot["qty"] = lot_qty - diff
                    diff = 0

        data[ticker] = q
        self._save(data)
        return True
