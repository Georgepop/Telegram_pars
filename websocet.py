import asyncio
import websockets
import json
from datetime import datetime
import requests
from typing import *
from mongopy import *


data = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo').json()  
symbols = [x["symbol"] for x in data["symbols"] if x['status']=='TRADING']
SYMBOLS = sorted([i.lower() for i in symbols if 'USDT' == i[-4:]])

# ✅ NEW ENDPOINT (required after 2026-04-23)
SOCKET_URL = "wss://fstream.binance.com/market/stream?streams=" + '/'.join([
    f'{symbol.lower()}@kline_1m' for symbol in SYMBOLS
])

insert_buffer: List[dict] = []
BATCH_SIZE = 1000


def flush_buffer(ws=None):
    global insert_buffer
    if not insert_buffer: return
    try:
        db['symbols'].insert_many(insert_buffer, ordered=False)
        insert_buffer.clear()
    except Exception as e:
        print(f"[DB Flush Error] {e}")


def parse_kline(data: dict) -> dict:
    """Parse kline — ONLY open, close, volume"""
    k = data['k']
    return {
        's': data['s'],
        'd': k['T']/1000,
        'h': float(k['h']),
        'l': float(k['l']),
        't': datetime.fromtimestamp(k['T'] / 1000),
        'o': float(k['o']),
        'c': float(k['c']),
        'v': float(k['v']),
        'is_closed': k['x'],
    }

async def futures_kline_stream():
    global insert_buffer
    while True:

        try:
            print(f"🔌 Connecting to")
            async with websockets.connect(
                SOCKET_URL,
                ping_interval=20,
                ping_timeout=10
            ) as ws:
                
                async for message in ws:
                    event = json.loads(message)['data']
                    if event.get('e') != 'kline':
                        continue
                    
                    kline = parse_kline(event)
                    if kline['is_closed']:
                        print(kline)
                        insert_buffer.append(kline)
                        
                        # 4. Batch insert & periodic cleanup
                        if len(insert_buffer) >= BATCH_SIZE:
                            flush_buffer(ws)
                                    # 🔥 Your strategy: on_candle_close(kline)

        except websockets.exceptions.InvalidStatusCode as e:
            print(f"❌ HTTP Error {e.status_code}: Check endpoint URL")
            break
        except websockets.exceptions.InvalidURI as e:
            print(f"❌ Invalid URI: {e}")
            break
        except ConnectionRefusedError:
            print("❌ Connection refused — check firewall/network")
        except Exception as e:
            print(f"⚠️ Error: {type(e).__name__}: {e}")
        
        print("🔄 Reconnecting in 3s...")
        await asyncio.sleep(3)

if __name__ == "__main__":
    print("🚀 Starting Binance Futures Stream (NEW ENDPOINT)...")
    asyncio.run(futures_kline_stream())