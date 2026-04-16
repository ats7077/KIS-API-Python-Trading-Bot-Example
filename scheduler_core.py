# ==========================================================
# [scheduler_core.py] - 🌟 100% 통합 완성본 (V27.20) 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.09 패치] API 결측치(None) 방어용 Safe Casting 전면 이식 완료
# 💡 [V24.10 수술] V_REV 동적 에스크로 차감 방어 (이중 차감 방지)
# 🚨 [V25.02 수술] 리버스 모드 일일 1회 확정 탈출 엔진 팩트 이식
# 🚨 [V27.12 그랜드 수술] 코파일럿 합작 - 리버스 하드스탑 부등호 논리 완벽 교정
# 🚨 [V27.13 그랜드 수술] 이벤트 루프 교착 방어 및 math.floor 평단가 왜곡 교정 완료
# 🚀 [V27.20 파이어게이트식 이식] 아침 09:01 확정 정산 졸업 카드 자동 출력 엔진 탑재
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
        return not schedule.empty
    except Exception as e:
        logging.error(f"⚠️ 달력 라이브러리 에러 발생: {e}")
        return True

# [기존 get_budget_allocation, get_actual_execution_price 함수 보존]
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
    matched_qty, total_amt = 0, 0.0
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
            if matched_qty >= target_qty: break
    if matched_qty > 0:
        return round(total_amt / matched_qty, 2)
    return 0.0

# ==========================================================
# 🚀 [핵심 추가] 아침 09:01 확정 정산 졸업 카드 자동화 스케줄러
# ==========================================================
async def scheduled_graduation_report(context):
    """
    매일 아침 09:01(KST)에 깨어나 증권사 배정 정산 결과를 확인하고 
    순수익이 확정된 0주 종목에 대해 단 1회 졸업카드를 발행합니다.
    """
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    
    # 시간 윈도우 방어 (09:01분대에만 작동)
    if not (now.hour == 9 and now.minute == 1):
        return

    app_data = context.job.data
    bot, cfg, broker, tx_lock = app_data['bot'], app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id

    try:
        async with tx_lock:
            # 1. 최신 계좌 잔고 확인
            _, holdings = await asyncio.to_thread(broker.get_account_balance)
        
        if holdings is None: return

        for t in cfg.get_active_tickers():
            # 브로커 잔고가 0주인 종목 스캔
            h_data = holdings.get(t) or {}
            qty = int(float(h_data.get('qty') or 0))
            
            # 장부상에는 데이터가 있는데 실제 잔고가 0주라면 '졸업 대상'
            ledger = cfg.get_ledger_by_ticker(t)
            if qty == 0 and ledger:
                logging.info(f"🎓 [{t}] 아침 확정 정산 스캔 시작...")
                
                # 2. 증권사 '확정 실현손익' API 호출 (파이어게이트식 정산 데이터 확보)
                # 이 시점(09:01)에는 한투 배치가 끝나 수수료/세금이 모두 반영되어 있음
                settled_pnl = await asyncio.to_thread(broker.get_realized_profit, t)
                
                if settled_pnl:
                    # 3. 확정 데이터를 바탕으로 졸업 카드 렌더링 및 자동 전송
                    # process_graduation 내부에서 명예의 전당 기록 및 장부 초기화(1회 한정) 수행
                    await bot.process_graduation(
                        ticker=t, 
                        chat_id=chat_id, 
                        context=context, 
                        settled_data=settled_pnl, 
                        auto_mode=True
                    )
                    logging.info(f"🏆 [{t}] 아침 확정 졸업 카드 자동 출력 완료")

    except Exception as e:
        logging.error(f"🚨 [scheduled_graduation_report] 에러: {e}")

# [나머지 scheduled_self_cleaning, scheduled_token_check, scheduled_force_reset, run_auto_sync 보존]

async def scheduled_self_cleaning(context):
    await asyncio.to_thread(perform_self_cleaning)
    logging.info("🧹 [시스템 자정 작업 완료] 7일 초과 로그/백업 및 24시간 초과 임시 파일 소각 완료")

async def scheduled_token_check(context):
    jitter_seconds = random.randint(0, 180)
    await asyncio.sleep(jitter_seconds)
    await asyncio.to_thread(context.job.data['broker']._get_access_token, force=True)
    logging.info("🔑 [API 토큰 갱신] 토큰 갱신 완료.")

async def scheduled_force_reset(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    if abs((now.hour * 60 + now.minute) - (target_hour * 60)) > 2: return
    if not is_market_open(): return
    
    try:
        app_data = context.job.data
        cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
        cfg.reset_locks()
        for t in cfg.get_active_tickers():
            if hasattr(cfg, 'set_order_locked'): cfg.set_order_locked(t, False)
        
        async with tx_lock:
            _, holdings = await asyncio.to_thread(broker.get_account_balance)
        holdings = holdings or {}
        msg_addons = ""
        HARD_STOP_THRESHOLDS = {"TQQQ": -15.0, "SOXL": -20.0}
        
        for t in cfg.get_active_tickers():
            rev_state = cfg.get_reverse_state(t)
            if rev_state.get("is_active"):
                curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                h_data = holdings.get(t) or {}
                actual_avg = float(h_data.get('avg') or 0.0)
                if curr_p > 0 and actual_avg > 0:
                    curr_ret = (curr_p - actual_avg) / actual_avg * 100.0
                    exit_threshold = HARD_STOP_THRESHOLDS.get(t, -20.0)
                    if curr_ret <= exit_threshold:
                        await asyncio.to_thread(broker.cancel_all_orders, t)
                        cfg.set_reverse_state(t, False, 0, 0.0)
                        cfg.clear_escrow_cash(t)
                        msg_addons += f"\n🚨 <b>[{t}] 하드스탑 탈출 (수익률: {curr_ret:.2f}%)</b>"
                    else: cfg.increment_reverse_day(t)
                else: cfg.increment_reverse_day(t)
                
        await context.bot.send_message(chat_id=context.job.chat_id, text=f"🔓 <b>[{target_hour}:00] 일일 초기화 완료</b>" + msg_addons, parse_mode='HTML')
    except Exception as e:
        logging.error(f"🚨 시스템 초기화 에러: {e}")

async def run_auto_sync(context, time_str):
    chat_id = context.job.chat_id
    bot = context.job.data['bot']
    status_msg = await context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[{time_str}] 장부 자동 동기화를 시작합니다.</b>", parse_mode='HTML')
    success_tickers = []
    for t in context.job.data['cfg'].get_active_tickers():
        res = await bot.process_auto_sync(t, chat_id, context, silent_ledger=True)
        if res == "SUCCESS": success_tickers.append(t)
    if success_tickers:
        async with context.job.data['tx_lock']:
            _, holdings = await asyncio.to_thread(context.job.data['broker'].get_account_balance)
        await bot._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
    else:
        await status_msg.edit_text(f"📝 <b>[{time_str}] 동기화 완료 (진행 장부 없음)</b>", parse_mode='HTML')
