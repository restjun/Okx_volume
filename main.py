from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd

app = FastAPI()

telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

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

def calculate_ema(close, period):
    if len(close) < period:
        return None
    return pd.Series(close).ewm(span=period, adjust=False).mean().iloc[-1]

def get_ema_with_retry(close, period):
    for _ in range(5):
        result = calculate_ema(close, period)
        if result is not None:
            return result
        time.sleep(0.5)
    return None

def get_ohlcv_okx(instId, bar='1H', limit=200):
    url = f"https://www.okx.com/api/v5/market/candles?instId={instId}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=[
            'ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'
        ])
        df['c'] = df['c'].astype(float)
        df['o'] = df['o'].astype(float)
        df['vol'] = df['vol'].astype(float)
        df['volCcyQuote'] = df['volCcyQuote'].astype(float)
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None

def get_all_timeframe_ema_status(inst_id):
    df = get_ohlcv_okx(inst_id, bar='4H', limit=300)
    if df is None:
        return "[4H]  📊:  ❌ 불러오기 실패"

    close = df['c'].astype(float).values

    ema_1 = get_ema_with_retry(close, 1)
    ema_2 = get_ema_with_retry(close, 2)
    ema_3 = get_ema_with_retry(close, 3)
    ema_5 = get_ema_with_retry(close, 5)
    ema_10 = get_ema_with_retry(close, 10)
    ema_15 = get_ema_with_retry(close, 15)
    ema_20 = get_ema_with_retry(close, 20)

    def check(cond):
        if cond is None:
            return "[❌]"
        return "[🟩]" if cond else "[🟥]"

    def safe_compare(a, b):
        if a is None or b is None:
            return None
        return a > b

    status_1_2 = check(safe_compare(ema_1, ema_2))
    status_2_3 = check(safe_compare(ema_2, ema_3))
    status_5_10 = check(safe_compare(ema_5, ema_10))
    status_10_15 = check(safe_compare(ema_10, ema_15))
    status_15_20 = check(safe_compare(ema_15, ema_20))

    return f"[4H]  📊:  {status_1_2}  {status_2_3}  {status_5_10}  {status_10_15}  {status_15_20}"

def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        df['datetime_kst'] = df['datetime'] + pd.Timedelta(hours=9)
        df.set_index('datetime_kst', inplace=True)
        daily = df.resample('1D', offset='9h').agg({
            'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'vol': 'sum'
        }).dropna().sort_index(ascending=False).reset_index()
        if len(daily) < 2:
            return None
        today_close = daily.loc[0, 'c']
        yesterday_close = daily.loc[1, 'c']
        return round(((today_close - yesterday_close) / yesterday_close) * 100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 1 else None
    except:
        return None

def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨🚨🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

def main():
    logging.info("📥 EMA 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    total_count = len(all_ids)

    volume_map = {}
    for inst_id in all_ids:
        vol_1h = calculate_1h_volume(inst_id)
        volume_map[inst_id] = vol_1h
        time.sleep(0.05)

    all_volume_data = sorted(volume_map.items(), key=lambda x: x[1], reverse=True)
    volume_rank_map = {inst_id: rank + 1 for rank, (inst_id, _) in enumerate(all_volume_data)}

    # 비트코인 정보 별도 추출
    btc_id = "BTC-USDT-SWAP"
    btc_vol = volume_map.get(btc_id, 0)
    btc_change = calculate_daily_change(btc_id)
    btc_ema_status = get_all_timeframe_ema_status(btc_id)
    btc_rank = volume_rank_map.get(btc_id, "N/A")
    btc_rank_display = f"⭐ {btc_rank}위" if isinstance(btc_rank, int) and btc_rank <= 3 else f"{btc_rank}위"
    btc_vol_str = format_volume_in_eok(btc_vol) or "🚫"

    # 거래대금 상위 3개 (비트코인 제외)
    top_volume_list = [item for item in all_volume_data if item[0] != btc_id][:3]

    top_list = []
    for inst_id, vol_1h in top_volume_list:
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_4h is None:
            continue
        daily_change = calculate_daily_change(inst_id)
        ema_status = get_all_timeframe_ema_status(inst_id)
        top_list.append((inst_id, vol_1h, daily_change, ema_status))

    # 메시지 조립
    message_lines = [
        f"💰 BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_vol_str})",
        btc_ema_status.strip(),
        f"🔢 랭킹: {btc_rank_display}",
        "━━━━━━━━━━━━━━━━━━━",
        "거래대금 상위 3종목",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    for i, (inst_id, vol_1h, daily_change, ema_status) in enumerate(top_list, 1):
        name = inst_id.replace("-USDT-SWAP", "")
        vol_str = format_volume_in_eok(vol_1h) or "🚫"
        rank = volume_rank_map.get(inst_id, "N/A")
        rank_display = f"⭐ {rank}위" if isinstance(rank, int) and rank <= 3 else f"{rank}위"

        message_lines.append(f"{i}. {name} {format_change_with_emoji(daily_change)} / 거래대금: ({vol_str})")
        message_lines.append(ema_status.strip())
        message_lines.append(f"🔢 랭킹: {rank_display}")
        message_lines.append("━━━━━━━━━━━━━━━━━━━")

    send_telegram_message("\n".join(message_lines))

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
