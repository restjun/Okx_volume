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

# ===== 텔레그램 전송 =====
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

# ===== API 호출 재시도 =====
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

# ===== OHLCV 가져오기 =====
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

# ===== EMA 계산 =====
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

# ===== EMA 상태 텍스트 =====
def get_ema_status_text_partial(df):
    close = df['c'].astype(float).values
    ema_5 = get_ema_with_retry(close, 5)
    ema_10 = get_ema_with_retry(close, 10)
    ema_15 = get_ema_with_retry(close, 15)
    ema_20 = get_ema_with_retry(close, 20)

    def check(cond):
        if cond is None:
            return "[❌]"
        return "[🟩]" if cond else "[🟥]"

    def safe_compare(a, b):
        if a is None or b is None:
            return None
        return a > b

    status_5_10 = check(safe_compare(ema_5, ema_10))
    status_10_15 = check(safe_compare(ema_10, ema_15))
    status_15_20 = check(safe_compare(ema_15, ema_20))

    return f"{status_5_10}{status_10_15}{status_15_20}"

def get_all_timeframe_ema_status(inst_id, bar='1D', limit=300):
    df = get_ohlcv_okx(inst_id, bar=bar, limit=limit)
    if df is None:
        return "[❌ 불러오기 실패]"
    return get_ema_status_text_partial(df)

# ===== EMA 정배열 여부 =====
def get_ema_bullish_status(inst_id):
    try:
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None:
            return False
        close_1d = df_1d['c'].values
        ema_5 = get_ema_with_retry(close_1d, 5)
        ema_10 = get_ema_with_retry(close_1d, 10)
        ema_15 = get_ema_with_retry(close_1d, 15)
        ema_20 = get_ema_with_retry(close_1d, 20)
        if None in [ema_5, ema_10, ema_15, ema_20]:
            return False
        return ema_5 > ema_10 > ema_15 > ema_20
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return False

# ===== 상승률 계산 =====
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

# ===== 거래대금 계산 =====
def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

def calculate_4h_volume_from_1h(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=4)  # 최근 1H 4개 합산
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

# ===== 전체 코인 리스트 =====
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# ===== 메시지 전송 =====
def send_ranked_volume_message(top_bullish, total_count, bullish_count, volume_rank_map_1h, volume_rank_map_4h):
    bearish_count = total_count - bullish_count
    bullish_ratio = bullish_count / total_count if total_count > 0 else 0

    if bullish_ratio >= 0.7:
        market_status = "📈 장이 좋음 (강세장)"
    elif bullish_ratio >= 0.4:
        market_status = "🔶 장 보통 (횡보장)"
    else:
        market_status = "📉 장이 안좋음 (약세장)"

    message_lines = [
        f"🟢 EMA 정배열: {bullish_count}개",
        f"🔴 EMA 역배열: {bearish_count}개",
        f"💡 시장 상태: {market_status}",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    # ===== BTC 정보 항상 표시 =====
    btc_id = "BTC-USDT-SWAP"
    btc_change = calculate_daily_change(btc_id) or 0
    btc_vol_1h = calculate_1h_volume(btc_id)
    btc_vol_4h = calculate_4h_volume_from_1h(btc_id)
    btc_ema_1d = get_all_timeframe_ema_status(btc_id, bar='1D')
    btc_ema_4h = get_all_timeframe_ema_status(btc_id, bar='1H')  # 4H 기준 EMA 대신 최근 1H 4개 합산

    btc_rank_1h = volume_rank_map_1h.get(btc_id, "N/A")
    btc_rank_4h = volume_rank_map_4h.get(btc_id, "N/A")
    btc_rank_display = f"⭐ {btc_rank_1h} / ⭐ {btc_rank_4h}" if isinstance(btc_rank_1h,int) else "N/A"

    message_lines += [
        f"💰 BTC (+{btc_change:.2f}%) / 거래대금: ({int(btc_vol_1h//1_000_000)}M / {int(btc_vol_4h//1_000_000)}M)",
        f"[1D] 📊: {btc_ema_1d}",
        f"[4H] 📊: {btc_ema_4h}",
        f"🔢 랭킹: {btc_rank_display}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    # ===== 조건 만족 종목 =====
    if top_bullish:
        message_lines.append("📈 1H 24시간 & 4H 4시간 거래대금 순위 10위 내 정배열")
        for i, (inst_id, vol_1h, change) in enumerate(top_bullish, 1):
            vol_4h = calculate_4h_volume_from_1h(inst_id)
            message_lines.append(f"{i}. {inst_id.replace('-USDT-SWAP','')} / 상승률: +{change:.2f}% / 거래대금: ({int(vol_1h//1_000_000)}M / {int(vol_4h//1_000_000)}M)")
        message_lines.append("━━━━━━━━━━━━━━━━━━━")
    else:
        message_lines.append("📉 조건 만족 종목이 없습니다.")

    send_telegram_message("\n".join(message_lines))

# ===== 메인 분석 =====
def main():
    logging.info("📥 EMA 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    total_count = len(all_ids)
    bullish_count = 0
    bullish_candidates = []

    # ===== 거래대금 계산 =====
    volume_map_1h = {}
    volume_map_4h = {}
    for inst_id in all_ids:
        volume_map_1h[inst_id] = calculate_1h_volume(inst_id)
        volume_map_4h[inst_id] = calculate_4h_volume_from_1h(inst_id)
        time.sleep(0.05)

    # ===== EMA 정배열 체크 =====
    for inst_id in all_ids:
        if get_ema_bullish_status(inst_id):
