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


# 🔹 MFI 계산
def calc_mfi(df, period=3):
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


# 🔹 RSI 계산
def calc_rsi(df, period=3):
    delta = df['c'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


# 🔹 타임프레임별 MFI/RSI 조건 체크
def check_mfi_rsi(inst_id, bar='1D', period=3, threshold=70):
    df = get_ohlcv_okx(inst_id, bar=bar, limit=100)
    if df is None or len(df) < period:
        return False
    mfi_val = calc_mfi(df, period).iloc[-1]
    rsi_val = calc_rsi(df, period).iloc[-1]
    if pd.isna(mfi_val) or pd.isna(rsi_val):
        return False
    return mfi_val >= threshold and rsi_val >= threshold


# 🔹 상승률 계산
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


# 🔹 OKX USDT-SWAP 심볼 가져오기
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]


# 🔹 24시간 거래대금 계산
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()


# 🔹 텔레그램 메시지 전송 (신규 코인 블록 별도)
def send_top_volume_message(top_ids, volume_map):
    global sent_signal_coins
    message_lines = [
        "⚡ 일봉/4H/1H 3봉 MFI·RSI ≥ 70 필터",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    rank_map = {inst_id: rank + 1 for rank, inst_id in enumerate(top_ids)}
    current_signal_coins = []

    for inst_id in top_ids:
        # 🔹 조건: 일봉 + 4H + 1H 모두 만족
        if not (
            check_mfi_rsi(inst_id, bar='1D') and
            check_mfi_rsi(inst_id, bar='4H') and
            check_mfi_rsi(inst_id, bar='1H')
        ):
            continue

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= 0:
            continue

        volume_24h = volume_map.get(inst_id, 0)
        actual_rank = rank_map.get(inst_id, "🚫")
        current_signal_coins.append((inst_id, daily_change, volume_24h, actual_rank))

    if current_signal_coins:
        # 신규 코인 확인
        new_coins = [c for c in current_signal_coins if c[0] not in sent_signal_coins]
        if not new_coins:
            logging.info("⚡ 신규 조건 코인 없음 → 메시지 전송 안 함")
            return
        # 신규 저장
        sent_signal_coins.update([c[0] for c in new_coins])

        # BTC 현황
        btc_id = "BTC-USDT-SWAP"
        btc_change = calculate_daily_change(btc_id)
        btc_volume = volume_map.get(btc_id, 0)
        btc_volume_str = format_volume_in_eok(btc_volume) or "🚫"

        message_lines += [
            "📌 BTC 현황",
            f"BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_volume_str})",
            "━━━━━━━━━━━━━━━━━━━"
        ]

        # 신규 코인 블록
        message_lines.append("🆕 신규 진입 코인")
        for inst_id, daily_change, volume_24h, actual_rank in new_coins:
            name = inst_id.replace("-USDT-SWAP", "")
            volume_str = format_volume_in_eok(volume_24h) or "🚫"
            message_lines.append(
                f"{name} {format_change_with_emoji(daily_change)} / 거래대금: ({volume_str}) {actual_rank}위"
            )
        message_lines.append("━━━━━━━━━━━━━━━━━━━")

        # 전체 TOP 10
        all_coins_to_send = current_signal_coins[:]
        all_coins_to_send.sort(key=lambda x: x[2], reverse=True)
        all_coins_to_send = all_coins_to_send[:10]

        message_lines.append("📊 전체 조건 만족 코인 TOP 10")
        for rank, (inst_id, daily_change, volume_24h, actual_rank) in enumerate(all_coins_to_send, start=1):
            name = inst_id.replace("-USDT-SWAP", "")
            volume_str = format_volume_in_eok(volume_24h) or "🚫"
            message_lines.append(
                f"{rank}. {name} {format_change_with_emoji(daily_change)} / 거래대금: ({volume_str}) {actual_rank}위"
            )
        message_lines.append("━━━━━━━━━━━━━━━━━━━")

        full_message = "\n".join(message_lines)
        send_telegram_message(full_message)
    else:
        logging.info("⚡ 조건 만족 코인 없음 → 메시지 전송 안 함")


def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    volume_map = {}

    for inst_id in all_ids:
        vol_24h = get_24h_volume(inst_id)
        volume_map[inst_id] = vol_24h
        time.sleep(0.05)

    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:100]
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
