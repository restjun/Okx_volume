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

# =========================================
# 텔레그램 메시지 전송 함수
# =========================================
def send_telegram_message(message):
    for retry_count in range(1, 11):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message, parse_mode="Markdown")
            logging.info("텔레그램 메시지 전송 성공: %s", message)
            return
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/10): %s", retry_count, str(e))
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")

# =========================================
# API 요청 재시도
# =========================================
def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt+1}/10): {str(e)}")
            time.sleep(5)
    return None

# =========================================
# EMA 계산
# =========================================
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

# =========================================
# OKX 심볼/데이터 불러오기
# =========================================
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

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

# =========================================
# EMA 상태 체크
# =========================================
def get_ema_bullish_status(inst_id):
    try:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=300)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1h is None or df_4h is None or df_1d is None:
            return None

        close_1h = df_1h['c'].values
        close_4h = df_4h['c'].values
        close_1d = df_1d['c'].values

        def get_emas(close):
            return (
                get_ema_with_retry(close, 5),
                get_ema_with_retry(close, 20),
                get_ema_with_retry(close, 50)
            )

        ema_1h = get_emas(close_1h)
        ema_4h = get_emas(close_4h)
        ema_1d = get_emas(close_1d)

        if None in ema_1h + ema_4h + ema_1d:
            return None

        def is_bullish(ema):
            return ema[0] > ema[1] > ema[2]  # 5 > 20 > 50

        return is_bullish(ema_1h) and is_bullish(ema_4h) and is_bullish(ema_1d)

    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return None

# =========================================
# 20~50 EMA 정배열 유지 캔들 수 계산
# =========================================
def count_ema_alignment_candles(close, short_period=20, long_period=50):
    ema_short = pd.Series(close).ewm(span=short_period, adjust=False).mean()
    ema_long = pd.Series(close).ewm(span=long_period, adjust=False).mean()

    count = 0
    for s, l in zip(reversed(ema_short), reversed(ema_long)):
        if pd.isna(s) or pd.isna(l):
            break
        if s > l:
            count += 1
        else:
            break
    return count

# =========================================
# 상승률 계산
# =========================================
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

# =========================================
# 거래대금 계산
# =========================================
def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

# =========================================
# 메인 분석 로직
# =========================================
def main():
    logging.info("📥 EMA 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    total_count = len(all_ids)
    bullish_list = []

    for inst_id in all_ids:
        is_bullish = get_ema_bullish_status(inst_id)
        if not is_bullish:
            continue

        df_1h = get_ohlcv_okx(inst_id, bar="1H", limit=100)
        if df_1h is None:
            continue
        close_1h = df_1h['c'].astype(float).values

        # ✅ 20~50 정배열 유지 캔들 수가 20 이하인 종목만 통과
        alignment_candles = count_ema_alignment_candles(close_1h, 20, 50)
        if alignment_candles > 20:
            continue

        ema_2 = get_ema_with_retry(close_1h, 2)
        ema_3 = get_ema_with_retry(close_1h, 3)
        if ema_2 is None or ema_3 is None or ema_2 >= ema_3:
            continue

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= 0:
            continue

        df_24h = get_ohlcv_okx(inst_id, bar="1D", limit=2)
        if df_24h is None:
            continue

        vol_24h = df_24h['volCcyQuote'].sum()
        bullish_list.append((inst_id, vol_24h, daily_change))
        time.sleep(0.1)

    top_bullish = sorted(bullish_list, key=lambda x: (x[1], x[2]), reverse=True)[:10]
    send_telegram_message(f"📊 조건 통과 종목 수: {len(top_bullish)} / 전체: {total_count}\n{top_bullish}")

# =========================================
# 스케줄러
# =========================================
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
