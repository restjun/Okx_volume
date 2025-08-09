from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd



import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import math
import telepot

# =========================
# 설정
# =========================
OKX_BASE_URL = "https://www.okx.com"
SYMBOLS_ENDPOINT = "/api/v5/public/instruments?instType=SWAP"
CANDLE_ENDPOINT = "/api/v5/market/candles"
VOLUME_ENDPOINT = "/api/v5/market/tickers?instType=SWAP"

telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)
MAX_WORKERS = 12

# =========================
# 유틸 함수
# =========================
def format_volume(value):
    """거래대금을 억, 조 단위로 변환"""
    value = float(value)
    if value >= 1e12:
        return f"{value / 1e12:.1f}조"
    elif value >= 1e8:
        return f"{value / 1e8:.1f}억"
    else:
        return f"{value:,.0f}"

def get_symbols():
    """OKX에서 전체 SWAP 심볼 목록 가져오기"""
    resp = requests.get(OKX_BASE_URL + SYMBOLS_ENDPOINT).json()
    return [item["instId"] for item in resp["data"]]

def get_candles(symbol, bar="1H", limit=200):
    """심볼의 캔들 데이터 가져오기"""
    url = f"{OKX_BASE_URL}{CANDLE_ENDPOINT}?instId={symbol}&bar={bar}&limit={limit}"
    resp = requests.get(url).json()
    df = pd.DataFrame(resp["data"], columns=["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"])
    df = df.astype({"o": float, "h": float, "l": float, "c": float})
    return df.iloc[::-1].reset_index(drop=True)  # 시간 오름차순

def calculate_ema(df, periods=[5, 20, 50]):
    """EMA 계산"""
    for p in periods:
        df[f"EMA{p}"] = df["c"].ewm(span=p, adjust=False).mean()
    return df

def is_bullish(df):
    """EMA 5 > EMA 20 > EMA 50 정배열 여부"""
    last = df.iloc[-1]
    return last.EMA5 > last.EMA20 > last.EMA50

def calculate_daily_change(symbol):
    """24시간 변동률 계산"""
    df = get_candles(symbol, bar="1D", limit=2)
    if len(df) < 2:
        return 0
    prev_close = df.iloc[-2]["c"]
    last_close = df.iloc[-1]["c"]
    return ((last_close - prev_close) / prev_close) * 100

def analyze_symbol(symbol):
    """심볼 분석 → 거래대금, 정배열 여부, 변동률"""
    try:
        df = get_candles(symbol)
        df = calculate_ema(df)
        bullish = is_bullish(df)
        change = calculate_daily_change(symbol)
        volume = float(df.iloc[-1]["volCcyQuote"])
        return {
            "symbol": symbol,
            "bullish": bullish,
            "change": change,
            "volume": volume
        }
    except Exception as e:
        return None

def get_ranked_results():
    """병렬 처리로 전체 심볼 분석"""
    symbols = get_symbols()
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_symbol, sym): sym for sym in symbols}
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
    return results

def send_ranked_volume_message(results):
    """메시지 전송"""
    total_count = len(results)
    bullish_list = [r for r in results if r["bullish"]]
    bullish_count = len(bullish_list)

    # 거래대금 순 정렬
    ranked = sorted(bullish_list, key=lambda x: x["volume"], reverse=True)[:10]

    msg = []
    msg.append("📊 OKX EMA 정배열 상위 코인")
    msg.append(f"총 심볼: {total_count}개 | EMA 정배열: {bullish_count}개")
    msg.append("")
    for idx, item in enumerate(ranked, 1):
        arrow = "📈" if item["change"] >= 0 else "📉"
        msg.append(f"{idx}. {item['symbol']} — {arrow} {item['change']:.2f}% — 거래대금: {format_volume(item['volume'])}")

    telepot.Bot(TOKEN).sendMessage(CHAT_ID, "\n".join(msg))

# =========================
# 실행
# =========================
if __name__ == "__main__":
    results = get_ranked_results()
    send_ranked_volume_message(results)
