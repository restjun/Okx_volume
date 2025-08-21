from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd

# =======================
# FastAPI & Telegram 설정
# =======================
app = FastAPI()
telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)
logging.basicConfig(level=logging.INFO)

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

# =======================
# 공통 함수
# =======================
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

# =======================
# OKX 데이터 수집
# =======================
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

# =======================
# EMA/RSI 분석
# =======================
def get_ema_bullish_status(inst_id):
    try:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=300)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1h is None or df_4h is None or df_1d is None:
            return None

        close_1h, close_4h, close_1d = df_1h['c'].values, df_4h['c'].values, df_1d['c'].values

        def is_bullish(close):
            ema3, ema5 = get_ema_with_retry(close, 3), get_ema_with_retry(close, 5)
            if ema3 is None or ema5 is None:
                return False
            return ema3 > ema5

        return is_bullish(close_1h) and is_bullish(close_4h) and is_bullish(close_1d)
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return None

def calculate_rsi(close, period=5):
    close = pd.Series(close)
    delta = close.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean().iloc[period-1]
    avg_loss = loss.rolling(window=period, min_periods=period).mean().iloc[period-1]
    for i in range(period, len(gain)):
        avg_gain = (avg_gain * (period - 1) + gain.iloc[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss.iloc[i]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# =======================
# 알림 메시지 생성
# =======================
def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

def check_market_and_alert():
    symbols = get_all_okx_swap_symbols()
    for symbol in symbols:
        bullish = get_ema_bullish_status(symbol)
        df_1h = get_ohlcv_okx(symbol, bar='1H', limit=50)
        if df_1h is None:
            continue
        rsi = calculate_rsi(df_1h['c'].values)
        if bullish and rsi < 30:
            msg = f"{symbol} 📈 EMA Bullish + RSI Oversold ({rsi:.2f})"
            send_telegram_message(msg)

# =======================
# 스케줄러
# =======================
def run_scheduler():
    schedule.every(1).minutes.do(check_market_and_alert)
    while True:
        schedule.run_pending()
        time.sleep(1)

# =======================
# FastAPI 엔드포인트
# =======================
@app.get("/")
def root():
    return {"message": "OKX EMA/RSI Telegram Bot Running"}

# =======================
# 스레드로 스케줄러 실행
# =======================
threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
