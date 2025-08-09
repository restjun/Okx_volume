
from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import logging
import pandas as pd

app = FastAPI()

# 텔레그램 봇 토큰 및 유저 ID
telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)


def send_telegram_message(message):
    """텔레그램 메시지 최대 10회 재시도 전송"""
    for retry_count in range(1, 11):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message)
            logging.info(f"텔레그램 메시지 전송 성공: {message}")
            return
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패 (재시도 {retry_count}/10): {e}")
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 초과")


def retry_request(func, *args, **kwargs):
    """API 요청 최대 10회 재시도, 429(빈도 제한) 시 1초 대기"""
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                logging.warning("API 호출 빈도 제한 429, 1초 대기 후 재시도")
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt+1}/10): {e}")
            time.sleep(5)
    return None


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
        return df.iloc[::-1]  # 시간 오름차순 정렬
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None


def get_ema(close, period):
    if len(close) < period:
        return None
    return pd.Series(close).ewm(span=period, adjust=False).mean().iloc[-1]


def get_ema_with_retry(close, period):
    for _ in range(5):
        ema = get_ema(close, period)
        if ema is not None:
            return ema
        time.sleep(0.5)
    return None


def get_ema_bullish_status(inst_id):
    try:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=300)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_1h is None or df_4h is None:
            return None

        close_1h = df_1h['c'].values
        close_4h = df_4h['c'].values

        def get_emas(close):
            return (
                get_ema_with_retry(close, 5),
                get_ema_with_retry(close, 20),
                get_ema_with_retry(close, 50)
            )

        ema_1h = get_emas(close_1h)
        ema_4h = get_emas(close_4h)

        if None in ema_1h + ema_4h:
            return None

        def is_bullish(ema):
            return ema[0] > ema[1] > ema[2]

        return is_bullish(ema_1h) and is_bullish(ema_4h)

    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return None


def get_ema_status_text(df, timeframe="1H"):
    close = df['c'].astype(float).values
    ema_2 = get_ema_with_retry(close, 2)
    ema_3 = get_ema_with_retry(close, 3)
    ema_5 = get_ema_with_retry(close, 5)
    ema_10 = get_ema_with_retry(close, 10)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)

    def check(cond):
        if cond is None:
            return "[❌]"
        return "[🟩]" if cond else "[🟥]"

    def safe_compare(a, b):
        if a is None or b is None:
            return None
        return a > b

    trend_status = [
        check(safe_compare(ema_5, ema_10)),  # 5-10
        check(safe_compare(ema_5, ema_20)),  # 5-20
        check(safe_compare(ema_20, ema_50)), # 20-50
        check(safe_compare(ema_50, ema_200)) # 50-200
    ]

    if timeframe == "1H":
        short_term_status = check(safe_compare(ema_2, ema_3))
        return f"[{timeframe}] 📊: {' '.join(trend_status)} / 🔄 {short_term_status}"
    else:
        return f"[{timeframe}] 📊: {' '.join(trend_status)}"


def get_all_timeframe_ema_status(inst_id):
    timeframes = {'4H': 300, '1H': 300}
    status_lines = []
    for tf, limit in timeframes.items():
        df = get_ohlcv_okx(inst_id, bar=tf, limit=limit)
        if df is not None:
            status = get_ema_status_text(df, timeframe=tf)
        else:
            status = f"[{tf}] 📊: ❌ 불러오기 실패"
        status_lines.append(status)
        time.sleep(0.2)
    return "\n".join(status_lines)


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


def job_check_and_notify():
    symbols = get_all_okx_swap_symbols()
    if not symbols:
        logging.error("심볼 목록을 불러오지 못함")
        return

    bullish_list = []
    bearish_list = []

    for inst_id in symbols:
        is_bullish = get_ema_bullish_status(inst_id)
        if is_bullish is None:
            continue

        daily_change = calculate_daily_change(inst_id)
        vol_1h = calculate_1h_volume(inst_id)
        vol_str = format_volume_in_eok(vol_1h) or "0"

        if is_bullish and vol_1h > 3_000_000_000:  # 30억 이상 거래량 조건 예시
            bullish_list.append((inst_id, daily_change, vol_str))
        else:
            bearish_list.append((inst_id, daily_change, vol_str))

        time.sleep(0.1)

    bullish_count = len(bullish_list)
    bearish_count = len(bearish_list)
    total_count = bullish_count + bearish_count

    msg = f"🔔 OKX 스왑 심볼 분석 결과\n"
    msg += f"총 심볼 수: {total_count}\n"
    msg += f"🟢 정배열(EMA) 심볼 수: {bullish_count}\n"
    msg += f"🔴 역배열(EMA) 심볼 수: {bearish_count}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"

    # 상위 10개 정배열 종목 정렬 (거래량+상승률 조합)
    bullish_sorted = sorted(bullish_list, key=lambda x: (x[2], x[1]), reverse=True)[:10]
    for inst_id, change, vol in bullish_sorted:
        msg += f"{inst_id}: {format_change_with_emoji(change)} 거래량:{vol}백만\n"

    send_telegram_message(msg)


def run_schedule():
    schedule.every(5).minutes.do(job_check_and_notify)

    while True:
        schedule.run_pending()
        time.sleep(1)


@app.get("/")
def read_root():
    return {"message": "OKX EMA Telegram Alert Server is Running"}


if __name__ == "__main__":
    # 스케줄러를 별도 스레드에서 실행
    thread = threading.Thread(target=run_schedule)
    thread.daemon = True
    thread.start()

    uvicorn.run(app, host="0.0.0.0", port=8000)
