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

# === 1D + 4H EMA 상태 한 줄 출력 ===
def get_ema_status_line(inst_id):
    try:
        rocket_flag = False  # 🚀 표시 조건

        # --- 1D EMA (5-10) ---
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None:
            daily_status = "[1D] ❌"
            ema_5_1d, ema_10_1d = None, None
        else:
            ema_5_1d = get_ema_with_retry(df_1d['c'].values, 5)
            ema_10_1d = get_ema_with_retry(df_1d['c'].values, 10)
            if None in [ema_5_1d, ema_10_1d]:
                daily_status = "[1D] ❌"
            else:
                status_5_10_1d = "🟩" if ema_5_1d > ema_10_1d else "🟥"
                daily_status = f"[1D] 📊: {status_5_10_1d}"

        # --- 4H EMA (5-10, 2-3) ---
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_4h is None:
            fourh_status = "[4H] ❌"
            ema_2_4h, ema_3_4h, ema_5_4h, ema_10_4h = None, None, None, None
        else:
            ema_2_4h = get_ema_with_retry(df_4h['c'].values, 2)
            ema_3_4h = get_ema_with_retry(df_4h['c'].values, 3)
            ema_5_4h = get_ema_with_retry(df_4h['c'].values, 5)
            ema_10_4h = get_ema_with_retry(df_4h['c'].values, 10)
            if None in [ema_2_4h, ema_3_4h, ema_5_4h, ema_10_4h]:
                fourh_status = "[4H] ❌"
            else:
                status_5_10_4h = "🟩" if ema_5_4h > ema_10_4h else "🟥"
                status_2_3_4h = "🟩" if ema_2_4h > ema_3_4h else "🟥"
                fourh_status = f"[4H] 📊: {status_5_10_4h} {status_2_3_4h}"

        # 🚀 조건: 일봉 5>10 정배열 + 4시간 5>10 정배열 + 4시간 2<3 역배열
        if (ema_5_1d and ema_10_1d and ema_5_1d > ema_10_1d) and \
           (ema_5_4h and ema_10_4h and ema_5_4h > ema_10_4h) and \
           (ema_2_4h and ema_3_4h and ema_2_4h < ema_3_4h):
            rocket_flag = True

        rocket_symbol = " 🚀" if rocket_flag else ""
        return f"{daily_status} | {fourh_status}{rocket_symbol}"
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return "[1D/4H] ❌"

# 나머지 코드 (calculate_daily_change, format_volume_in_eok, format_change_with_emoji, calculate_1h_volume, send_ranked_volume_message, get_all_okx_swap_symbols, get_ema_bullish_status, main, run_scheduler 등) 은 원본 그대로 유지
# ...

@app.on_event("startup")
def start_scheduler():
    schedule.every(1).minutes.do(main)
    threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
