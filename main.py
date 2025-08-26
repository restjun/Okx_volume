lfrom fastapi import FastAPI
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# 🔹 OKX OHLCV 데이터 가져오기
def get_ohlcv_okx(instId, bar="1H", limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={instId}&bar={bar}&limit={limit}"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        if "data" not in data:
            return None
        df = pd.DataFrame(data["data"],
                          columns=["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"])
        df = df.astype({"o": float, "h": float, "l": float, "c": float, "vol": float,
                        "volCcy": float, "volCcyQuote": float})
        df = df.iloc[::-1].reset_index(drop=True)  # 최신이 마지막으로 오게
        return df
    except Exception as e:
        logging.error(f"OHLCV 가져오기 실패 {instId}: {e}")
        return None

# 🔹 거래대금 24시간치 계산 (1H * 24)
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df["volCcyQuote"].sum()

# 🔹 전체 심볼 가져오기
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        return [x["instId"] for x in data["data"] if x["instId"].endswith("-USDT-SWAP")]
    except Exception as e:
        logging.error(f"심볼 가져오기 실패: {e}")
        return []

# 🔹 메시지 전송
def send_message(text):
    try:
        bot.sendMessage(telegram_user_id, text)
    except Exception as e:
        logging.error(f"텔레그램 전송 실패: {e}")

# 🔹 상위 거래대금 메시지 전송
def send_top_volume_message(top_ids, volume_map):
    msg = "📊 거래대금 상위 100 코인\n\n"
    for i, inst_id in enumerate(top_ids, start=1):
        vol = volume_map.get(inst_id, 0)
        msg += f"{i}. {inst_id}  ({vol/1e8:.2f} 억 USDT)\n"
    send_message(msg)

# 🔹 메인 로직
def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    volume_map = {}

    for inst_id in all_ids:
        vol_24h = get_24h_volume(inst_id)   # 🔹 24시간 거래대금
        volume_map[inst_id] = vol_24h
        time.sleep(0.05)

    # 🔹 거래대금 기준 상위 100개 추출
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:100]
    send_top_volume_message(top_ids, volume_map)

# 🔹 스케줄러 실행
def run_schedule():
    schedule.every(60).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)

# 🔹 서버 실행 (FastAPI + 스케줄 병렬 실행)
@app.on_event("startup")
def on_startup():
    threading.Thread(target=run_schedule, daemon=True).start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
