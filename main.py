from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import numpy as np

app = FastAPI()

telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

# 🔹 전역 변수: 마지막 돌파 상태 저장
sent_signal_coins = {}

# 🔹 텔레그램 메시지 전송
def send_telegram_message(message):
    for retry_count in range(1, 11):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message)
            logging.info("텔레그램 메시지 전송 성공")
            return
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패 (재시도 {retry_count}/10): {e}")
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 초과")

# 🔹 API 재시도 함수
def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt+1}/10): {e}")
            time.sleep(5)
    return None

# 🔹 OKX OHLCV 가져오기
def get_ohlcv_okx(instId, bar='1H', limit=200):
    url = f"https://www.okx.com/api/v5/market/candles?instId={instId}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=[
            'ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'
        ])
        for col in ['o', 'h', 'l', 'c', 'vol', 'volCcyQuote']:
            df[col] = df[col].astype(float)
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None

# 🔹 Wilder's RMA (TradingView RSI/MFI와 동일)
def rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()

# 🔹 RSI 계산 (TradingView 기본)
def calc_rsi(df, period=5):
    delta = df['c'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# 🔹 MFI 계산 (TradingView 기본)
def calc_mfi(df, period=5):
    tp = (df['h'] + df['l'] + df['c']) / 3
    mf = tp * df['vol']

    delta_tp = tp.diff()
    positive_mf = mf.where(delta_tp > 0, 0.0)
    negative_mf = mf.where(delta_tp < 0, 0.0)

    pos_rma = rma(positive_mf, period)
    neg_rma = rma(negative_mf, period)

    mfi = 100 * pos_rma / (pos_rma + neg_rma)
    return mfi

# 🔹 RSI/MFI 포맷
def format_rsi_mfi(value):
    if pd.isna(value):
        return "(N/A)"
    return f"🟢 {value:.1f}" if value >= 70 else f"🔴 {value:.1f}"

# 🔹 일봉 MFI & RSI 돌파 체크 (5일선 기준)
def check_daily_mfi_rsi_cross(inst_id, period=5, threshold=70):
    df = get_ohlcv_okx(inst_id, bar='1D', limit=100)
    if df is None or len(df) < period + 1:
        return False
    mfi = calc_mfi(df, period)
    rsi = calc_rsi(df, period)
    prev_mfi, curr_mfi = mfi.iloc[-2], mfi.iloc[-1]
    prev_rsi, curr_rsi = rsi.iloc[-2], rsi.iloc[-1]
    if pd.isna(curr_mfi) or pd.isna(curr_rsi):
        return False
    return (curr_mfi >= threshold and curr_rsi >= threshold and (prev_mfi < threshold or prev_rsi < threshold))

# 🔹 상승률 계산
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        df['datetime_kst'] = df['datetime'] + pd.Timedelta(hours=9)
        df.set_index('datetime_kst', inplace=True)
        daily = df.resample('1D', offset='9h').agg({
            'o':'first','h':'max','l':'min','c':'last','vol':'sum'
        }).dropna().sort_index(ascending=False).reset_index()
        if len(daily) < 2:
            return None
        today_close = daily.loc[0, 'c']
        yesterday_close = daily.loc[1, 'c']
        return round(((today_close - yesterday_close)/yesterday_close)*100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

# 🔹 거래대금 단위
def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 1 else "🚫"
    except:
        return "🚫"

# 🔹 상승률 이모지
def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨🚨🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

# 🔹 OKX USDT-SWAP 심볼
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# 🔹 24시간 거래대금
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

# 🔹 신규 돌파 메시지 (상위 3개만, 거래대금 순위 포함)
def send_new_entry_message(all_ids):
    global sent_signal_coins
    volume_map = {inst_id:get_24h_volume(inst_id) for inst_id in all_ids}
    rank_map = {inst_id: rank+1 for rank, inst_id in enumerate(sorted(volume_map, key=volume_map.get, reverse=True))}

    new_entry_coins = []

    for inst_id in all_ids:
        is_cross = check_daily_mfi_rsi_cross(inst_id)
        df_daily = get_ohlcv_okx(inst_id, bar="1D", limit=100)
        if df_daily is None or len(df_daily)<5:
            continue

        daily_mfi = calc_mfi(df_daily,5).iloc[-1]
        daily_rsi = calc_rsi(df_daily,5).iloc[-1]

        if pd.isna(daily_mfi) or daily_mfi<70 or pd.isna(daily_rsi) or daily_rsi<70:
            continue

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change<=0:
            continue

        last_status = sent_signal_coins.get(inst_id, False)
        if not last_status and is_cross:
            volume_24h = volume_map.get(inst_id,0)
            coin_rank = rank_map.get(inst_id,"🚫")
            new_entry_coins.append((inst_id, daily_change, volume_24h, daily_mfi, daily_rsi, coin_rank))

        sent_signal_coins[inst_id] = is_cross

    if new_entry_coins:
        new_entry_coins.sort(key=lambda x: x[2], reverse=True)
        new_entry_coins = new_entry_coins[:3]

        message_lines = ["⚡ 일봉 MFI·RSI 5일선 ≥ 70 필터", "━━━━━━━━━━━━━━━━━━━"]
        btc_id = "BTC-USDT-SWAP"
        btc_change = calculate_daily_change(btc_id)
        btc_volume = volume_map.get(btc_id,0)
        btc_volume_str = format_volume_in_eok(btc_volume)
        message_lines += [
            "📌 BTC 현황",
            f"BTC\n거래대금: {btc_volume_str}\n상승률: {format_change_with_emoji(btc_change)}",
            "━━━━━━━━━━━━━━━━━━━",
            "🆕 신규 진입 코인 (상위 3개)"
        ]

        for inst_id,daily_change,volume_24h,daily_mfi,daily_rsi,coin_rank in new_entry_coins:
            name = inst_id.replace("-USDT-SWAP","")
            volume_str = format_volume_in_eok(volume_24h)
            message_lines.append(
                f"{name}\n거래대금: {volume_str}\n순위: {coin_rank}위\n상승률: {format_change_with_emoji(daily_change)}\n"
                f"📊 일봉 RSI: {format_rsi_mfi(daily_rsi)} / MFI: {format_rsi_mfi(daily_mfi)}"
            )

        message_lines.append("━━━━━━━━━━━━━━━━━━━")
        send_telegram_message("\n".join(message_lines))
    else:
        logging.info("⚡ 신규 진입 없음 → 메시지 전송 안 함")

# 🔹 메인 실행
def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    send_new_entry_message(all_ids)

# 🔹 스케줄러
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
def start_scheduler():
    schedule.every(1).minutes.do(main)
    threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
