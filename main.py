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
        df['h'] = df['h'].astype(float)
        df['l'] = df['l'].astype(float)
        df['vol'] = df['vol'].astype(float)
        df['volCcyQuote'] = df['volCcyQuote'].astype(float)
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None


# 🔹 트레이딩뷰 호환 MFI 계산 함수
def calc_mfi(df, period=14):  # ✅ 기본값 14로 변경
    tp = (df['h'] + df['l'] + df['c']) / 3
    rmf = tp * df['vol']

    positive_mf = []
    negative_mf = []
    for i in range(1, len(df)):
        if tp.iloc[i] > tp.iloc[i-1]:
            positive_mf.append(rmf.iloc[i])
            negative_mf.append(0)
        elif tp.iloc[i] < tp.iloc[i-1]:
            positive_mf.append(0)
            negative_mf.append(rmf.iloc[i])
        else:
            positive_mf.append(0)
            negative_mf.append(0)

    positive_mf = pd.Series([None] + positive_mf, index=df.index)
    negative_mf = pd.Series([None] + negative_mf, index=df.index)

    pos_mf_sum = positive_mf.rolling(window=period, min_periods=period).sum()
    neg_mf_sum = negative_mf.rolling(window=period, min_periods=period).sum()

    mfi = 100 * (pos_mf_sum / (pos_mf_sum + neg_mf_sum))
    return mfi


# 🔹 MFI 1시간봉 → 14일선으로 수정
def get_mfi_status_line(inst_id, period=14, mfi_threshold=70):  # ✅ 기본값 14
    try:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=100)
        if df_1h is None or len(df_1h) < period:
            return "[1H MFI] ❌", False

        mfi_series = calc_mfi(df_1h, period)

        if mfi_series.iloc[-2] < mfi_threshold <= mfi_series.iloc[-1]:
            return f"[1H MFI] 🚨 MFI 돌파: {mfi_series.iloc[-1]:.2f}", True
        else:
            return f"[1H MFI] {mfi_series.iloc[-1]:.2f}", False

    except Exception as e:
        logging.error(f"{inst_id} MFI 계산 실패: {e}")
        return "[1H MFI] ❌", False


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
        "⚡  1H MFI 14일선 70 이상 돌파 코인",  # ✅ 문구도 14일선으로 변경
        "━━━━━━━━━━━━━━━━━━━",
    ]

    rank_map = {inst_id: rank + 1 for rank, inst_id in enumerate(top_ids)}
    current_signal_coins = []

    for inst_id in top_ids:
        mfi_status_line, signal_flag = get_mfi_status_line(inst_id, period=14, mfi_threshold=70)  # ✅ period=14
        if not signal_flag:
            continue
        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= -100:
            continue
        volume_1h = volume_map.get(inst_id, 0)
        actual_rank = rank_map.get(inst_id, "🚫")
        current_signal_coins.append((inst_id, mfi_status_line, daily_change, volume_1h, actual_rank))

    if current_signal_coins:
        current_signal_coins.sort(key=lambda x: x[3], reverse=True)

        btc_id = "BTC-USDT-SWAP"
        btc_change = calculate_daily_change(btc_id)
        btc_volume = volume_map.get(btc_id, 0)
        btc_volume_str = format_volume_in_eok(btc_volume) or "🚫"
        btc_mfi_line, _ = get_mfi_status_line(btc_id, period=14, mfi_threshold=70)  # ✅ period=14

        btc_lines = [
            "📌 BTC 현황",
            f"BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_volume_str})",
            btc_mfi_line,
            "━━━━━━━━━━━━━━━━━━━"
        ]
        message_lines += btc_lines

        for rank, (inst_id, mfi_line, daily_change, volume_1h, actual_rank) in enumerate(current_signal_coins, start=1):
            name = inst_id.replace("-USDT-SWAP", "")
            volume_str = format_volume_in_eok(volume_1h) or "🚫"
            message_lines.append(
                f"{rank}. {name} {format_change_with_emoji(daily_change)} / 거래대금: ({volume_str}) {actual_rank}위"
            )
            message_lines.append(mfi_line)
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
