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

# ==== EMA 상태 메시지(1D) ====
def get_ema_status_text_partial_daily(inst_id):
    try:
        df = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df is None:
            return "[1D] ❌ 불러오기 실패"

        close_prices = df['c'].values
        ema_5 = get_ema_with_retry(close_prices, 5)
        ema_20 = get_ema_with_retry(close_prices, 20)
        if None in [ema_5, ema_20]:
            return "[1D] ❌ 데이터 부족"

        status_5_20 = "🟩" if ema_5 > ema_20 else "🟥"

        return f"[1D] 📊: {status_5_20}"
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return "[1D] ❌ 오류"

def get_all_timeframe_ema_status(inst_id):
    return get_ema_status_text_partial_daily(inst_id)

# ==== EMA 상태 메시지(4H) ====
def get_ema_status_text_partial_4h(inst_id):
    try:
        df = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df is None:
            return "[4H] ❌ 불러오기 실패"

        close_prices = df['c'].values
        ema_1 = get_ema_with_retry(close_prices, 1)
        ema_3 = get_ema_with_retry(close_prices, 3)
        ema_5 = get_ema_with_retry(close_prices, 5)
        ema_20 = get_ema_with_retry(close_prices, 20)

        if None in [ema_1, ema_3, ema_5, ema_20]:
            return "[4H] ❌ 데이터 부족"
            
        status_1_3 = "🟥" if ema_1 < ema_3 else "🟩"
        status_5_20 = "🟩" if ema_5 > ema_20 else "🟥"

        return f"[4H] 📊: {status_1_3}/{status_5_20}"
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return "[4H] ❌ 오류"

def get_all_timeframe_ema_status_4h(inst_id):
    return get_ema_status_text_partial_4h(inst_id)

def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        df['datetime_kst'] = df['datetime'] + pd.Timedelta(hours=9)
        df.set_index('datetime_kst', inplace=True)
        daily = df.resample('1D', offset='9h').agg({
            'o': 'first',
            'h': 'max',
            'l': 'min',
            'c': 'last',
            'vol': 'sum'
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

# ==== 정배열 기준 수정 ====
def get_ema_bullish_status(inst_id):
    """일봉 5-20 정배열"""
    try:
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None:
            return False
        close_1d = df_1d['c'].values
        ema_5 = get_ema_with_retry(close_1d, 5)
        ema_20 = get_ema_with_retry(close_1d, 20)
        if None in [ema_5, ema_20]:
            return False
        return ema_5 > ema_20
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return False

def get_ema_bullish_status_4h(inst_id):
    """4시간 1-3 역배열 + 5-20 정배열"""
    try:
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_4h is None:
            return False
        close_4h = df_4h['c'].values
        ema_1 = get_ema_with_retry(close_4h, 1)
        ema_3 = get_ema_with_retry(close_4h, 3)
        ema_5 = get_ema_with_retry(close_4h, 5)
        ema_20 = get_ema_with_retry(close_4h, 20)
        if None in [ema_1, ema_3, ema_5, ema_20]:
            return False
        return (ema_1 < ema_3) and (ema_5 > ema_20)
    except Exception as e:
        logging.error(f"{inst_id} 4H EMA 상태 계산 실패: {e}")
        return False

def send_ranked_volume_message(top_bullish, total_count, bullish_count, volume_rank_map, all_volume_data):
    bearish_count = total_count - bullish_count
    bullish_ratio = bullish_count / total_count if total_count > 0 else 0

    if bullish_ratio >= 0.7:
        market_status = "📈 장이 좋음 (강세장)"
    elif bullish_ratio >= 0.4:
        market_status = "🔶 장 보통 (횡보장)"
    else:
        market_status = "📉 장이 안좋음 (약세장)"

    message_lines = [
        f"🟢 EMA 정배열: {bullish_count}개",
        f"🔴 EMA 역배열: {bearish_count}개",
        f"💡 시장 상태: {market_status}",
        "━━━━━━━━━━━━━━━━━━━",
        "🎯 코인지수 비트코인 + [일봉 정배열 5-20 / 4H 1-3역 + 5-20]",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    btc_id = "BTC-USDT-SWAP"
    btc_ema_status = get_all_timeframe_ema_status(btc_id)
    btc_ema_status_4h = get_all_timeframe_ema_status_4h(btc_id)
    btc_change = calculate_daily_change(btc_id)
    btc_volume = dict(all_volume_data).get(btc_id, 0)
    btc_volume_str = format_volume_in_eok(btc_volume) or "🚫"
    btc_rank = volume_rank_map.get(btc_id, "N/A")
    btc_rank_display = f"⭐ {btc_rank}위" if isinstance(btc_rank, int) and btc_rank <= 3 else f"{btc_rank}위"

    message_lines += [
        f"💰 BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_volume_str})",
        btc_ema_status.strip(),
        btc_ema_status_4h.strip(),
        f"🔢 랭킹: {btc_rank_display}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    send_telegram_message("\n".join(message_lines))

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

    bullish_count_only = 0
    bullish_list = []
    volume_map = {}

    for inst_id in all_ids:
        vol_1h = calculate_1h_volume(inst_id)
        volume_map[inst_id] = vol_1h
        time.sleep(0.05)

    for inst_id in all_ids:
        if get_ema_bullish_status(inst_id) and get_ema_bullish_status_4h(inst_id):
            bullish_count_only += 1
        time.sleep(0.05)

    send_ranked_volume_message([], total_count, bullish_count_only, {}, volume_map)

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
