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

def get_ema_status_text_partial(df):
    close = df['c'].astype(float).values

    ema_5 = get_ema_with_retry(close, 5)
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

    status_5_20 = check(safe_compare(ema_5, ema_20))
    status_20_50 = check(safe_compare(ema_20, ema_50))
    status_50_200 = check(safe_compare(ema_50, ema_200))

    return f"[4H]  📊:  {status_5_20}  {status_20_50}  {status_50_200}"

def get_all_timeframe_ema_status(inst_id):
    df = get_ohlcv_okx(inst_id, bar='4H', limit=300)
    if df is None:
        return "[4H]  📊:  ❌ 불러오기 실패"
    return get_ema_status_text_partial(df)

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

def send_ranked_volume_message(top_bullish, total_count, bullish_count, volume_rank_map, all_volume_data):
    bearish_count = total_count - bullish_count
    message_lines = [
        f"🟢 EMA 정배열: {bullish_count}개",
        f"🔴 EMA 역배열: {bearish_count}개",
        "━━━━━━━━━━━━━━━━━━━",
        "🎯 코인지수 비트코인 + 거래대금 24시간",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    btc_id = "BTC-USDT-SWAP"
    btc_ema_status = get_all_timeframe_ema_status(btc_id)
    btc_change = calculate_daily_change(btc_id)
    btc_volume = dict(all_volume_data).get(btc_id, 0)
    btc_volume_str = format_volume_in_eok(btc_volume) or "🚫"
    btc_rank = volume_rank_map.get(btc_id, "N/A")
    btc_rank_display = f"⭐  {btc_rank}위" if isinstance(btc_rank, int) and btc_rank <= 3 else f"{btc_rank}위"

    message_lines += [
        f"💰 BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_volume_str})",
        btc_ema_status,
        f"🔢  랭킹:  {btc_rank_display}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    if all_volume_data:
        top_inst_id, top_vol = all_volume_data[0]
        top_change = calculate_daily_change(top_inst_id)
        top_ema_status = get_all_timeframe_ema_status(top_inst_id)
        top_name = top_inst_id.replace("-USDT-SWAP", "")
        top_vol_str = format_volume_in_eok(top_vol) or "🚫"
        top_rank = volume_rank_map.get(top_inst_id, "N/A")
        top_rank_display = f"⭐  {top_rank}위" if isinstance(top_rank, int) and top_rank <= 3 else f"{top_rank}위"

        message_lines += [
            "🏆 실시간 거래대금 1위",
            f"1. {top_name} {format_change_with_emoji(top_change)} / 거래대금: ({top_vol_str})",
            top_ema_status,
            f"🔢  랭킹:  {top_rank_display}",
            "━━━━━━━━━━━━━━━━━━━"
        ]

    filtered_top_bullish = []
    for item in top_bullish:
        inst_id = item[0]
        volume_1h = dict(all_volume_data).get(inst_id, 0)
        rank = volume_rank_map.get(inst_id)
        if volume_1h < 1_000_000 or rank is None or rank > 30:
            continue
        filtered_top_bullish.append((inst_id, item[1], item[2], volume_1h, rank))

    if filtered_top_bullish:
        message_lines.append("📈 [정배열 + 거래대금]")
        for i, (inst_id, _, change, volume_1h, rank) in enumerate(filtered_top_bullish, 1):
            name = inst_id.replace("-USDT-SWAP", "")
            ema_status = get_all_timeframe_ema_status(inst_id)
            volume_str = format_volume_in_eok(volume_1h) or "🚫"
            rank_display = f"⭐  {rank}위" if rank <= 3 else f"{rank}위"
            ema_lines = ema_status.split("\n")
            message_lines += [
                f"{i}. {name} {format_change_with_emoji(change)} / 거래대금: ({volume_str})",
                ema_lines[0],
                ema_lines[1] if len(ema_lines) > 1 else "",
                f"🔢  랭킹:  {rank_display}",
                "━━━━━━━━━━━━━━━━━━━"
            ]
    else:
        message_lines.append("📉 정배열 종목이 없습니다.")

    send_telegram_message("\n".join(message_lines))

def main():
    logging.info("📥 EMA 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    total_count = len(all_ids)
    bullish_count_only = 0
    bullish_list = []

    volume_map = {}
    for inst_id in all_ids:
        vol_1h = calculate_1h_volume(inst_id)
        volume_map[inst_id] = vol_1h
        time.sleep(0.05)

    for inst_id in all_ids:
        # EMA 상태 계산 (전체 정배열 개수 계산용)
        ema_status = get_ema_bullish_status(inst_id)
        if ema_status:
            bullish_count_only += 1
        time.sleep(0.05)

    for inst_id in all_ids:
        # 정배열(5-20-50-200) 전체 조건 대신 부분적으로 EMA 상태 체크
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_4h is None:
            continue

        # 거래대금, 상승률 계산
        vol_1h = volume_map.get(inst_id, 0)
        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= 0:
            continue

        # EMA 상태 부분 표시 (부족해도 무시하지 않고 메시지에 포함)
        ema_text = get_ema_status_text_partial(df_4h)
        # 정배열 여부 판단 (5 > 20 > 50 기준만 간단 체크)
        ema_5 = get_ema_with_retry(df_4h['c'].values, 5)
        ema_20 = get_ema_with_retry(df_4h['c'].values, 20)
        ema_50 = get_ema_with_retry(df_4h['c'].values, 50)
        if ema_5 is None or ema_20 is None or ema_50 is None:
            continue  # EMA 5-20-50 데이터 부족하면 무시

        if ema_5 > ema_20 > ema_50 and vol_1h >= 1_000_000:
            bullish_list.append((inst_id, vol_1h, daily_change))

    top_bullish = sorted(bullish_list, key=lambda x: (x[1], x[2]), reverse=True)[:30]

    all_volume_data = sorted(volume_map.items(), key=lambda x: x[1], reverse=True)
    volume_rank_map = {inst_id: rank + 1 for rank, (inst_id, _) in enumerate(all_volume_data)}

    send_ranked_volume_message(top_bullish, total_count, bullish_count_only, volume_rank_map, all_volume_data)

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
