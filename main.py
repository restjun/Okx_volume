from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import logging
import pandas as pd

app = FastAPI()

# ====== Telegram Bot 설정 ======
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

# ====== 요청 재시도 함수 ======
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

# ====== OHLCV & EMA 계산 ======
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

def get_ema_icon(close):
    ema_3 = get_ema_with_retry(close, 3)
    ema_5 = get_ema_with_retry(close, 5)
    if ema_3 is None or ema_5 is None:
        return "[❌]"
    return "[🟩]" if ema_3 > ema_5 else "[🟥]"

def get_all_timeframe_ema_status(inst_id):
    try:
        df_1h = get_ohlcv_okx(inst_id, bar="1H", limit=100)
        df_4h = get_ohlcv_okx(inst_id, bar="4H", limit=100)
        df_1d = get_ohlcv_okx(inst_id, bar="1D", limit=100)
        if df_1h is None or df_4h is None or df_1d is None:
            return None
        return {
            "1H": get_ema_icon(df_1h['c'].values),
            "4H": get_ema_icon(df_4h['c'].values),
            "1D": get_ema_icon(df_1d['c'].values)
        }
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 가져오기 실패: {e}")
        return None

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
        return str(eok) if eok >= 0 else None
    except:
        return None

def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

# ====== OKX 심볼 가져오기 ======
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# ====== 알림 스케줄러 ======
def check_and_notify():
    symbols = get_all_okx_swap_symbols()
    for inst_id in symbols:
        ema_status = get_all_timeframe_ema_status(inst_id)
        if ema_status is None:
            continue

        df = get_ohlcv_okx(inst_id, bar="1H", limit=1)
        if df is None or len(df) == 0:
            continue
        volume_eok = format_volume_in_eok(df['vol'].iloc[-1])
        change = calculate_daily_change(inst_id)
        change_str = format_change_with_emoji(change)

        if volume_eok is not None and int(volume_eok) >= 300:  # 300억 이상 필터
            message = f"{inst_id}\nEMA: {ema_status['1H']} | {ema_status['4H']} | {ema_status['1D']}\n거래대금: {volume_eok}억\n변동률: {change_str}"
            send_telegram_message(message)

def run_scheduler():
    schedule.every(1).minutes.do(check_and_notify)
    while True:
        schedule.run_pending()
        time.sleep(1)

# ====== 백그라운드 스레드로 스케줄러 실행 ======
threading.Thread(target=run_scheduler, daemon=True).start()

@app.get("/")
def read_root():
    return {"status": "OK, Telegram bot running"}
