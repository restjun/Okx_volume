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
        # --- 1D EMA (5-10, 5-20) ---
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None:
            daily_status = "[1D] ❌"
            condition_1d_5_10 = False
            condition_1d_5_20 = False
        else:
            ema_5_1d = get_ema_with_retry(df_1d['c'].values, 5)
            ema_10_1d = get_ema_with_retry(df_1d['c'].values, 10)
            ema_20_1d = get_ema_with_retry(df_1d['c'].values, 20)
            if None in [ema_5_1d, ema_10_1d, ema_20_1d]:
                daily_status = "[1D] ❌"
                condition_1d_5_10 = False
                condition_1d_5_20 = False
            else:
                condition_1d_5_10 = ema_5_1d > ema_10_1d
                condition_1d_5_20 = ema_5_1d > ema_20_1d
                status_5_20_1d = "🟩" if condition_1d_5_20 else "🟥"
                daily_status = f"[1D] 📊: {status_5_20_1d}"

        # --- 4H EMA (5-10, 1-3, 5-20) ---
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_4h is None:
            fourh_status = "[4H] ❌"
            condition_5_10_4h = False
            condition_1_3_4h = False
            condition_5_20_4h = False
        else:
            ema_1_4h = get_ema_with_retry(df_4h['c'].values, 1)
            ema_3_4h = get_ema_with_retry(df_4h['c'].values, 3)
            ema_5_4h = get_ema_with_retry(df_4h['c'].values, 5)
            ema_10_4h = get_ema_with_retry(df_4h['c'].values, 10)
            ema_20_4h = get_ema_with_retry(df_4h['c'].values, 20)
            if None in [ema_1_4h, ema_3_4h, ema_5_4h, ema_10_4h, ema_20_4h]:
                fourh_status = "[4H] ❌"
                condition_5_10_4h = False
                condition_1_3_4h = False
                condition_5_20_4h = False
            else:
                condition_5_10_4h = ema_5_4h > ema_10_4h
                condition_1_3_4h = ema_1_4h < ema_3_4h  # 역배열 조건
                condition_5_20_4h = ema_5_4h > ema_20_4h
                status_1_3_4h = "🟩" if ema_1_4h > ema_3_4h else "🟥"
                status_5_20_4h = "🟩" if condition_5_20_4h else "🟥"
                fourh_status = f"[4H] 📊: {status_5_20_4h} {status_1_3_4h}"

        # --- 조건 체크 후 🚀 붙이기 ---
        rocket = ""
        if condition_1d_5_20 and condition_5_20_4h and condition_1_3_4h:
            rocket = " 🚀🚀🚀"   # 기존 조건
        elif condition_1d_5_20 and condition_5_20_4h:
            rocket = " "     # 새로운 조건

        return f"{daily_status} | {fourh_status}{rocket}"
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return "[1D/4H] ❌"

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

def send_top10_volume_message(top_10_ids, volume_map):
    message_lines = [
        "🎯 거래대금 상위 1~10 종목",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    for i, inst_id in enumerate(top_10_ids, 1):
        name = inst_id.replace("-USDT-SWAP", "")
        ema_status_line = get_ema_status_line(inst_id)
        daily_change = calculate_daily_change(inst_id)
        volume_1h = volume_map.get(inst_id, 0)
        volume_str = format_volume_in_eok(volume_1h) or "🚫"

        message_lines.append(f"{i}. {name} {format_change_with_emoji(daily_change)} / 거래대금: ({volume_str})")
        message_lines.append(ema_status_line)
        message_lines.append("━━━━━━━━━━━━━━━━━━━")

    send_telegram_message("\n".join(message_lines))

def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# ===== main() =====
def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    volume_map = {}

    # 거래대금 먼저 계산
    for inst_id in all_ids:
        vol_1h = calculate_1h_volume(inst_id)
        volume_map[inst_id] = vol_1h
        time.sleep(0.05)

    # 거래대금 TOP 10 추출
    top_10_ids = [inst_id for inst_id, _ in sorted(volume_map.items(), key=lambda x: x[1], reverse=True)[:20]]

    # 메시지 전송
    send_top10_volume_message(top_10_ids, volume_map)

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
