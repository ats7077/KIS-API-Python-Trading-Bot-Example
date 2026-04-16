# ==========================================================
# [scheduler_core.py] - 🌟 100% 통합 완성본 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.09 패치] API 결측치(None) 방어용 Safe Casting 전면 이식 완료
# 💡 [V24.10 수술] V_REV 동적 에스크로 차감 방어 (이중 차감 방지)
# 🚨 [V25.02 수술] 리버스 모드 일일 1회 확정 탈출(TQQQ -15% / SOXL -20%) 엔진 팩트 이식
# 🚨 [V25.19 핫픽스] 자정(Midnight) 래핑(Wrap-around) 시간 오차 수학적 교정
# 🚨 [V25.19 핫픽스] 리버스 확정 탈출 시 무조건 누적(increment)되던 데드코드 분리 차단
# 🚨 [V27.12 그랜드 수술] 코파일럿 합작 - 리버스 하드스탑 부등호 논리 반전(수익 시 탈출) 완벽 교정 및 비활성 종목의 누적일 오염(State Corruption) 원천 차단
# 🚨 [V27.13 그랜드 수술] 이벤트 루프 교착(Deadlock) 방어, TOCTOU 시차 불일치 해소, 미체결 잔여 주문(Orphan) 선제 취소, math.floor 가짜 수익(Phantom PnL) 교정 완료
# ==========================================================
import os
import logging
import datetime
import pytz
import time
import math
import asyncio
import glob
import random
import pandas_market_calendars as mcal

def is_dst_active():
    est = pytz.timezone('US/Eastern')
    return datetime.datetime.now(est).dst() != datetime.timedelta(0)

def get_target_hour():
    return (17, "🌞 서머타임 적용(여름)") if is_dst_active() else (18, "❄️ 서머타임 해제(겨울)")

def is_market_open():
    try:
        est = pytz.timezone('US/Eastern')
        today = datetime.datetime.now(est)
        if today.weekday() >= 5: 
            return False
            
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=today.date(), end_date=today.date())
        
        if not schedule.empty:
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"⚠️ 달력 라이브러리 에러 발생. 평일이므로 강제 개장 처리합니다: {e}")
        return True

def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    
    safe_cash = float(cash) if cash is not None else 0.0
    
    dynamic_total_locked = 0.0
    for tx in tickers:
        rev_state = cfg.get_reverse_state(tx)
        if rev_state.get("is_active", False):
            is_locked = getattr(cfg, 'get_order_locked', lambda x: False)(tx)
            if not is_locked:
                dynamic_total_locked += float(cfg.get_escrow_cash(tx) or 0.0)

    free_cash = max(0.0, safe_cash - dynamic_total_locked)
    
    for tx in sorted_tickers:
        rev_state = cfg.get_reverse_state(tx)
        is_rev = rev_state.get("is_active", False)
        
        other_locked = dynamic_total_locked
        if is_rev:
            is_locked = getattr(cfg, 'get_order_locked', lambda x: False)(tx)
            if not is_locked:
                other_locked -= float(cfg.get_escrow_cash(tx) or 0.0)
        
        if is_rev:
            my_escrow = float(cfg.get_escrow_cash(tx) or 0.0)
            allocated[tx] = my_escrow + other_locked
        else:
            split = int(cfg.get_split_count(tx) or 0)
            seed = float(cfg.get_seed(tx) or 0.0)
            portion = seed / split if split > 0 else 0.0
            
            if free_cash >= portion:
                allocated[tx] = free_cash + other_locked
                free_cash -= portion
            else: 
                allocated[tx] = other_locked
                
    return sorted_tickers, allocated

def get_actual_execution_price(execs, target_qty, side_cd):
    if not execs: return 0.0
    
    execs.sort(key=lambda x: str(x.get('ord_tmd') or '000000'), reverse=True)
    matched_qty = 0
    total_amt = 0.0
    for ex in execs:
        if ex.get('sll_buy_dvsn_cd') == side_cd: 
            eqty = int(float(ex.get('ft_ccld_qty') or 0))
            eprice = float(ex.get('ft_ccld_unpr3') or 0.0)
            if matched_qty + eqty <= target_qty:
                total_amt += eqty * eprice
                matched_qty += eqty
            elif matched_qty < target_qty:
                rem = target_qty - matched_qty
                total_amt += rem * eprice
                matched_qty += rem
            
            if matched_qty >= target_qty:
                break
    
    if matched_qty > 0:
        # MODIFIED: [math.floor 평단가 왜곡 교정] 무조건 내림으로 인한 평단가 축소(Phantom PnL) 왜곡 방지를 위해 표준 반올림(round) 적용
        return round(total_amt / matched_qty, 2)
    return 0.0

def perform_self_cleaning():
    try:
        now = time.time()
        seven_days = 7 * 24 * 3600
        one_day = 24 * 3600
        
        for f in glob.glob("logs/*.log"):
            if os.path.isfile(f) and os.stat(f).st_mtime < now - seven_days:
                try: os.remove(f)
                except: pass
                
        for f in glob.glob("data/*.bak_*"):
            if os.path.isfile(f) and os.stat(f).st_mtime < now - seven_days:
                try: os.remove(f)
                except: pass
                
        for directory in ["data", "logs"]:
            for f in glob.glob(f"{directory}/tmp*"):
                if os.path.isfile(f) and os.stat(f).st_mtime < now - one_day:
                    try: os.remove(f)
                    except: pass
    except Exception as e:
        logging.error(f"🧹 자정(Self-Cleaning) 작업 중 오류 발생: {e}")

async def scheduled_self_cleaning(context):
    await asyncio.to_thread(perform_self_cleaning)
    logging.info("🧹 [시스템 자정 작업 완료] 7일 초과 로그/백업 및 24시간 초과 임시 파일 소각 완료")

async def scheduled_token_check(context):
    jitter_seconds = random.randint(0, 180)
    logging.info(f"🔑 [API 토큰 갱신] 서버 동시 접속 부하 방지를 위해 {jitter_seconds}초 대기 후 발급을 시작합니다.")
    await asyncio.sleep(jitter_seconds)
    
    await asyncio.to_thread(context.job.data['broker']._get_access_token, force=True)
    logging.info("🔑 [API 토큰 갱신] 토큰 갱신이 안전하게 완료되었습니다.")

# ==========================================================
# 🚨 [V25.02 핵심 수술] 리버스 모드 절대 하드스탑(TQQQ -15% / SOXL -20%) 확정 탈출 엔진 이식
# 💡 가변 변수(exit_target) 의존성 100% 적출 및 타임 패러독스 원천 차단
# ==========================================================

async def scheduled_force_reset(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    
    now_minutes = now.hour * 60 + now.minute
    target_minutes = target_hour * 60
    
    diff = min((now_minutes - target_minutes) % 1440, (target_minutes - now_minutes) % 1440)
    if diff > 2:
        return
        
    if not is_market_open():
        await context.bot.send_message(chat_id=context.job.chat_id, text="⛔ <b>오늘은 미국 증시 휴장일입니다. 금일 시스템 매매 잠금 해제 및 정규장 주문 스케줄을 모두 건너뜁니다.</b>", parse_mode='HTML')
        return
    
    try:
        app_data = context.job.data
        cfg = app_data['cfg']
        broker = app_data['broker']
        tx_lock = app_data['tx_lock']
        chat_id = context.job.chat_id
        
        cfg.reset_locks()
        
        for t in cfg.get_active_tickers():
            if hasattr(cfg, 'set_order_locked'):
                cfg.set_order_locked(t, False)
        
        msg_addons = ""
        
        # NEW: [하드코딩된 임계치 방어] 종목별 하드스탑 명시적 매핑 딕셔너리 선언
        HARD_STOP_THRESHOLDS = {
            "TQQQ": -15.0,
            "SOXL": -20.0
        }
        
        for t in cfg.get_active_tickers():
            rev_state = cfg.get_reverse_state(t)
            
            # 🚨 [수술 완료] 리버스 모드가 켜진(Active) 종목만 탈출 검사 및 누적일 카운팅 수행 (상태 오염 방지)
            if rev_state.get("is_active"):
                
                # MODIFIED: [TOCTOU 시차 불일치 및 이벤트 루프 교착 방어] 잔고와 현재가를 동일 락 안에서 비동기로 동시 스냅샷 확보
                async with tx_lock:
                    _, holdings_snap = await asyncio.to_thread(broker.get_account_balance)
                    curr_p = await asyncio.to_thread(broker.get_current_price, t)
                
                h_data = (holdings_snap or {}).get(t) or {}
                actual_avg = float(h_data.get('avg') or 0.0)
                curr_p = float(curr_p or 0.0)
                
                if curr_p > 0 and actual_avg > 0:
                    curr_ret = (curr_p - actual_avg) / actual_avg * 100.0
                    
                    # MODIFIED: [하드코딩 방어] 등록되지 않은 종목 유입 시 잘못된 청산 방지를 위해 Fail-loud 가동
                    exit_threshold = HARD_STOP_THRESHOLDS.get(t)
                    if exit_threshold is None:
                        logging.error(f"🚨 [FATAL] {t}에 대한 하드스탑 임계치가 설정되지 않았습니다. 즉시 확인 바랍니다.")
                        continue
                    
                    # 🚨 [수술 완료] 하드스탑 탈출 부등호 논리 반전 교정 (손실이 임계치보다 크거나 같을 때 탈출)
                    if curr_ret <= exit_threshold:
                        
                        # NEW: [미체결 잔여 주문 방치(Orphan Orders) 차단] 장부 초기화 전 증권사 서버의 미체결 주문 선제적 전량 취소
                        try:
                            cancelled = await asyncio.to_thread(broker.cancel_all_orders, t)
                            logging.warning(f"🚨 [HardStop] {t} 미체결 주문 {cancelled}건 취소 완료")
                        except Exception as cancel_err:
                            logging.error(f"🚨 [HardStop] {t} 주문 취소 실패 — 수동 확인 필수: {cancel_err}")
                            await context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>[{t}] 하드스탑 주문 취소 실패!</b> 브로커에서 미체결 주문을 수동으로 확인하세요.", parse_mode='HTML')

                        cfg.set_reverse_state(t, False, 0, 0.0)
                        cfg.clear_escrow_cash(t)
                        
                        ledger_data = cfg.get_ledger()
                        changed = False
                        for lr in ledger_data:
                            if lr.get('ticker') == t and lr.get('is_reverse', False):
                                lr['is_reverse'] = False
                                changed = True
                        if changed:
                            cfg._save_json(cfg.FILES["LEDGER"], ledger_data)
                            
                        msg_addons += f"\n🚨 <b>[{t}] 하드스탑 확정 탈출 발동 (수익률: {curr_ret:.2f}% <= 기준: {exit_threshold}%)!</b>\n▫️ 격리 병동을 즉시 폐쇄하고 V14 본대로 완벽히 복귀했습니다."
                    else:
                        # 하드스탑에 도달하지 않았으므로 누적일 정상 카운팅
                        cfg.increment_reverse_day(t)
                else:
                    # 가격을 못 불러와도 리버스 모드이므로 누적일은 카운팅
                    cfg.increment_reverse_day(t)
            
            # 🚨 [수술 완료] 리버스 모드가 아닌 정상 종목은 else 블록을 완전히 삭제하여 누적일 오염 원천 차단
                
        final_msg = f"🔓 <b>[{target_hour}:00] 시스템 일일 초기화 완료 (매매 잠금 해제 & 팩트 스캔)</b>" + msg_addons
        await context.bot.send_message(chat_id=chat_id, text=final_msg, parse_mode='HTML')
        
    except Exception as e:
        await context.bot.send_message(chat_id=context.job.chat_id, text=f"🚨 <b>시스템 초기화 중 에러 발생:</b> {e}", parse_mode='HTML')

async def scheduled_auto_sync_summer(context):
    if not is_dst_active(): return 
    await run_auto_sync(context, "08:30")

async def scheduled_auto_sync_winter(context):
    if is_dst_active(): return 
    await run_auto_sync(context, "09:30")

async def run_auto_sync(context, time_str):
    chat_id = context.job.chat_id
    bot = context.job.data['bot']
    status_msg = await context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[{time_str}] 장부 자동 동기화(무결성 검증)를 시작합니다.</b>", parse_mode='HTML')
    
    success_tickers = []
    for t in context.job.data['cfg'].get_active_tickers():
        res = await bot.process_auto_sync(t, chat_id, context, silent_ledger=True)
        if res == "SUCCESS":
            success_tickers.append(t)
            
    if success_tickers:
        # MODIFIED: [이벤트 루프 교착 방어] 동기 API 호출을 비동기 래퍼로 위임하여 봇 마비 원천 차단
        async with context.job.data['tx_lock']:
            _, holdings = await asyncio.to_thread(context.job.data['broker'].get_account_balance)
        await bot._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
    else:
        await status_msg.edit_text(f"📝 <b>[{time_str}] 장부 동기화 완료</b> (표시할 진행 중인 장부가 없습니다)", parse_mode='HTML')
