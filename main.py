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


def get_ema_status_line(inst_id):
    try:
        # 1D EMA 3-5 계산
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None:
            daily_status = "[1D] ❌"
            daily_ok_long = False
            daily_ok_short = False
        else:
            closes_1d = df_1d['c'].values
            ema_3_1d = get_ema_with_retry(closes_1d, 3)
            ema_5_1d = get_ema_with_retry(closes_1d, 5)
            if None in [ema_3_1d, ema_5_1d]:
                daily_status = "[1D] ❌"
                daily_ok_long = daily_ok_short = False
            else:
                if ema_3_1d > ema_5_1d:
                    daily_status = "[1D] 3-5 🟩"
                    daily_ok_long = True
                    daily_ok_short = False
                else:
                    daily_status = "[1D] 3-5 🟥"
                    daily_ok_long = False
                    daily_ok_short = True

        # 1H EMA 3-5로 변경
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=50)
        if df_1h is None or len(df_1h) < 5:  # 최소 5개 필요
            oneh_status = "[1H] ❌"
            golden_cross = False
            dead_cross = False
        else:
            closes_1h = df_1h['c'].values
            ema_3_series = pd.Series(closes_1h).ewm(span=3, adjust=False).mean()
            ema_5_series = pd.Series(closes_1h).ewm(span=5, adjust=False).mean()
            golden_cross = ema_3_series.iloc[-2] <= ema_5_series.iloc[-2] and ema_3_series.iloc[-1] > ema_5_series.iloc[-1]
            dead_cross = ema_3_series.iloc[-2] >= ema_5_series.iloc[-2] and ema_3_series.iloc[-1] < ema_5_series.iloc[-1]
            oneh_status = f"[1H] 📊: {'🟩' if ema_3_series.iloc[-1] > ema_5_series.iloc[-1] else '🟥'}"

        # 롱/숏 신호 판단 (1H 기준)
        if daily_ok_long and golden_cross:
            signal_type = "long"
            signal = " 🚀🚀🚀(롱)"
        elif daily_ok_short and dead_cross:
            signal_type = "short"
            signal = " ⚡⚡⚡(숏)"
        else:
            signal_type = None
            signal = ""

        return f"{daily_status} | {oneh_status}{signal}", signal_type

    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return "[1D/1H] ❌", None


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


def send_top_volume_message(top_ids, volume_map):
    message_lines = [
        "⚡  3-5 추세매매 거래대금 순서",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    current_signal_coins = []

    for inst_id in top_ids:
        ema_status_line, signal_type = get_ema_status_line(inst_id)
        if signal_type is None:
            continue
        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= 0:
            continue
        current_signal_coins.append(inst_id)

    if current_signal_coins:
        btc_id = "BTC-USDT-SWAP"
        btc_change = calculate_daily_change(btc_id)
        btc_volume = volume_map.get(btc_id, 0)
        btc_volume_str = format_volume_in_eok(btc_volume) or "🚫"
        btc_status_line, _ = get_ema_status_line(btc_id)

        btc_lines = [
            "📌 BTC 현황",
            f"BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_volume_str})",
            btc_status_line,
            "━━━━━━━━━━━━━━━━━━━"
        ]
        message_lines += btc_lines

        for inst_id in current_signal_coins:
            name = inst_id.replace("-USDT-SWAP", "")
            ema_status_line, _ = get_ema_status_line(inst_id)
            daily_change = calculate_daily_change(inst_id)
            volume_1h = volume_map.get(inst_id, 0)
            volume_str = format_volume_in_eok(volume_1h) or "🚫"
            rank = top_ids.index(inst_id) + 1
            message_lines.append(f"{rank}. {name} {format_change_with_emoji(daily_change)} / 거래대금: ({volume_str})")
            message_lines.append(ema_status_line)
            message_lines.append("━━━━━━━━━━━━━━━━━━━")

        full_message = "\n".join(message_lines)
        send_telegram_message(full_message)
    else:
        logging.info("⚡ 신규 조건 만족 코인 없음 → 메시지 전송 안 함")


def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]


def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    volume_map = {}
    for inst_id in all_ids:
        vol_1h = calculate_1h_volume(inst_id)
        volume_map[inst_id] = vol_1h
        time.sleep(0.05)
    top_ids = [inst_id for inst_id, _ in sorted(volume_map.items(), key=lambda x: x[1], reverse=True)[:20]]
    send_top_volume_message(top_ids, volume_map)


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
