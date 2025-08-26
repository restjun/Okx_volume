from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import numpy as np

app = FastAPI()

telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

# 🔹 전역 변수: 이미 메시지 전송한 코인 저장
sent_signal_coins = set()

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


def get_ohlcv_okx(instId, bar='1D', limit=100):
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


# 🔹 MFI 계산 함수
def calc_mfi(df, period=5):
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

    positive_mf = pd.Series([np.nan] + positive_mf, index=df.index)
    negative_mf = pd.Series([np.nan] + negative_mf, index=df.index)

    pos_mf_sum = positive_mf.rolling(window=period, min_periods=period).sum()
    neg_mf_sum = negative_mf.rolling(window=period, min_periods=period).sum()

    mfi = 100 * (pos_mf_sum / (pos_mf_sum + neg_mf_sum))
    return mfi


# 🔹 RSI 계산 함수
def calc_rsi(df, period=5):
    delta = df['c'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


# 🔹 일봉 조건 체크 함수 (5일선 MFI & RSI ≥ 70)
def check_daily_mfi_rsi(inst_id, period=5, threshold=70):
    df_1d = get_ohlcv_okx(inst_id, bar="1D", limit=100)
    if df_1d is None or len(df_1d) < period:
        return False
    mfi_val = calc_mfi(df_1d, period).iloc[-1]
    rsi_val = calc_rsi(df_1d, period).iloc[-1]
    if pd.isna(mfi_val) or pd.isna(rsi_val):
        return False
    return mfi_val >= threshold and rsi_val >= threshold


# 🔹 상승률 계산
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=10)
    if df is None or len(df) < 2:
        return None
    today_close = df['c'].iloc[-1]
    yesterday_close = df['c'].iloc[-2]
    return round(((today_close - yesterday_close) / yesterday_close) * 100, 2)


def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨🚨🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"


# 🔹 OKX USDT-SWAP 심볼 가져오기
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]


# 🔹 텔레그램 메시지 전송 (일봉 조건만)
def send_daily_signal_message():
    global sent_signal_coins
    all_ids = get_all_okx_swap_symbols()
    current_signal_coins = []

    for inst_id in all_ids:
        if not check_daily_mfi_rsi(inst_id, period=5, threshold=70):
            continue

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= 0:
            continue

        current_signal_coins.append((inst_id, daily_change))

    if current_signal_coins:
        new_coins = [c[0] for c in current_signal_coins if c[0] not in sent_signal_coins]
        if not new_coins:
            logging.info("⚡ 신규 조건 코인 없음 → 메시지 전송 안 함")
            return

        sent_signal_coins.update(new_coins)
        message_lines = ["⚡ 일봉 5일선 MFI/RSI≥70 필터"]
        message_lines.append("━━━━━━━━━━━━━━━━━━━")
        for rank, (inst_id, daily_change) in enumerate(current_signal_coins, start=1):
            name = inst_id.replace("-USDT-SWAP", "")
            message_lines.append(f"{rank}. {name} {format_change_with_emoji(daily_change)}")
            message_lines.append("━━━━━━━━━━━━━━━━━━━")
        full_message = "\n".join(message_lines)
        send_telegram_message(full_message)
    else:
        logging.info("⚡ 신규 조건 만족 코인 없음 → 메시지 전송 안 함")


def main():
    logging.info("📥 일봉 조건 분석 시작")
    send_daily_signal_message()


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
