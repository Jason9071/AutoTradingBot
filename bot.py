import os
from dotenv import load_dotenv # type: ignore
load_dotenv()
from line_notify import LineNotify # type: ignore
import time
import schedule # type: ignore
from binance.um_futures import UMFutures # type: ignore
from binance.lib.utils import config_logging # type: ignore
from datetime import datetime, timezone, timedelta
import json

def send_message_on_line_notify(new_data):

    file_path = 'data.json'

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    open_time_exists = any(item["Open time"] == new_data["Open time"] for item in data)

    if open_time_exists:
        print(f"已存在相同的 Open time: {new_data['Open time']}")
    else:
        data.append(new_data)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"新的數據已成功寫入，Open time: {new_data['Open time']}")


        LINE_ACCESS_TOKEN = os.getenv('LINE_ACCESS_TOKEN')
        notify = LineNotify(LINE_ACCESS_TOKEN)
        mseeage = "\n【爆量通知】 \n" + new_data["Open time"] + "\n" + "ETH/USDT Perp 5min volume is over 40k\n"
        notify.send(mseeage)

def convert_data_by_start(data, start, end):
    keys = [
        "Open time",          # Open time
        "Open price",               # Open price
        "High price",               # Highest price
        "Low price",                # Lowest price
        "Close price",              # Close price (current price if Kline is not closed)
        "Volume",             # Trading volume
        "Close time",         # Close time
        "Quote asset volume", # Quote asset volume
        "Number of trades",   # Number of trades
        "Taker buy base asset volume",  # Taker buy base asset volume
        "Taker buy quote asset volume", # Taker buy quote asset volume
        "Ignore"              # Ignore this field
    ]
    converted_data = []
    currend = start
    while currend < end :
        row_dict = dict(zip(keys, data[currend]))
        row_dict["Open time"] = datetime.fromtimestamp(row_dict["Open time"] / 1000, timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        row_dict["Close time"] = datetime.fromtimestamp(row_dict["Close time"] / 1000, timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        converted_data.append(row_dict)
        currend += 1
    return converted_data

def fetch_and_convert_data(pair, window, threshold):
    print(f"Task running at {datetime.now()}")
    
    um_futures_client = UMFutures()
    kline_datas = um_futures_client.klines(pair, window)

    lastest_5m_kline = convert_data_by_start(kline_datas, len(kline_datas) -1, len(kline_datas))[-1]

    if float(lastest_5m_kline["Volume"]) >= threshold : 
        send_message_on_line_notify(lastest_5m_kline)
    else :
        print(lastest_5m_kline)
  

schedule.every(1).seconds.do(fetch_and_convert_data, pair="ETHUSDT", window="5m", threshold= 40000)

while True:
    schedule.run_pending()
    time.sleep(1)