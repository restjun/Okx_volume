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
            bot.sendMessage(chat_id=telegram_user_id, text=message, parse_mode="Markdown")
            logging.info("텔레그램 메시지 전송 성공: %s", message)
            return
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/10): %s", retry_count, str(e))
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")

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

def calculate_rsi(close, period=14):
    if len(close) < period:
        return None
    series = pd.Series(close)
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else None

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
            return ema[0] > ema[1] > ema[2]

        return is_bullish(ema_1h) and is_bullish(ema_4h) and is_bullish(ema_1d)

    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return None

def get_ema_status_text(df, timeframe="1H"):
    close = df['c'].astype(float).values

    ema_5 = get_ema_with_retry(close, 5)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)
    ema_2 = get_ema_with_retry(close, 2)
    ema_3 = get_ema_with_retry(close, 3)
    rsi_14 = calculate_rsi(close, 14)

    def check(cond):
        if cond is None:
            return "[❌]"
        return "[🟩]" if cond else "[🟥]"

    def safe_compare(a, b):
        if a is None or b is None:
            return None
        return a > b

    trend_status = [
        check(safe_compare(ema_5, ema_20)),
        check(safe_compare(ema_20, ema_50)),
        check(safe_compare(ema_50, ema_200))
    ]
    short_term_status = check(safe_compare(ema_2, ema_3))
    rsi_text = f"{rsi_14:.2f}" if rsi_14 is not None else "N/A"

    return f"[{timeframe}] 📊: {' '.join(trend_status)} / 🔄 {short_term_status} / RSI: {rsi_text}"

def get_all_timeframe_ema_status(inst_id):
    timeframes = {'1D': 250, '4H': 300, '1H': 300, '15m': 300}
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

def send_ranked_volume_message(top_bullish, total_count, bullish_count):
    bearish_count = total_count - bullish_count

    message_lines = [
        f"📊 전체 조회 코인 수: {total_count}개",
        f"🟢 EMA 정배열: {bullish_count}개",
        f"🔴 EMA 역배열: {bearish_count}개",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    btc_id = "BTC-USDT-SWAP"
    btc_ema_status = get_all_timeframe_ema_status(btc_id)
    btc_change = calculate_daily_change(btc_id)
    btc_volume = calculate_1h_volume(btc_id)
    btc_volume_str = format_volume_in_eok(btc_volume) or "🚫"

    message_lines += [
        "🎯 코인지수 비트코인",
        "━━━━━━━━━━━━━━━━━━━",
        f"💰 BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_volume_str})",
        f"{btc_ema_status}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    filtered_top_bullish = []
    for item in top_bullish:
        inst_id = item[0]
        volume_1h = calculate_1h_volume(inst_id)
        if volume_1h < 1_000_000:
            continue
        filtered_top_bullish.append((inst_id, item[1], item[2], volume_1h))

    if filtered_top_bullish:
        message_lines.append("📈 [정배열 + 거래대금 TOP (1000만 이상)]")
        for i, (inst_id, _, change, volume_1h) in enumerate(filtered_top_bullish, 1):
            name = inst_id.replace("-USDT-SWAP", "")
            ema_status = get_all_timeframe_ema_status(inst_id)
            volume_str = format_volume_in_eok(volume_1h) or "🚫"
            message_lines += [
                f"*{i}. {name}* {format_change_with_emoji(change)} / 거래대금: ({volume_str})\n{ema_status}",
                "━━━━━━━━━━━━━━━━━━━"
            ]
    else:
        message_lines.append("📉 거래대금 1000만 이상인 정배열 종목이 없습니다.")

    send_telegram_message("\n".join(message_lines))

def main():
    logging.info("📥 EMA 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    total_count = len(all_ids)
    bullish_list = []

    for inst_id in all_ids:
        is_bullish = get_ema_bullish_status(inst_id)
        if not is_bullish:
            continue

        # ✅ 추가된 1시간봉 EMA 2 < EMA 3 (역배열) 필터
        df_1h = get_ohlcv_okx(inst_id, bar="1H", limit=300)
        if df_1h is None:
            continue
        close_1h = df_1h['c'].values
        ema_2 = get_ema_with_retry(close_1h, 2)
        ema_3 = get_ema_with_retry(close_1h, 3)
        if ema_2 is None or ema_3 is None or ema_2 > ema_3:
            continue

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= -100:
            continue

        df_24h = get_ohlcv_okx(inst_id, bar="1D", limit=2)
        if df_24h is None:
            continue

        vol_24h = df_24h['volCcyQuote'].sum()
        bullish_list.append((inst_id, vol_24h, daily_change))
        time.sleep(0.1)

    top_bullish = sorted(bullish_list, key=lambda x: (x[1], x[2]), reverse=True)[:1]
    send_ranked_volume_message(top_bullish, total_count, len(bullish_list))

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
