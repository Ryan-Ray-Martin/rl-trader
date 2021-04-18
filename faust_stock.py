from dataclasses import asdict, dataclass
import json
import requests
import time
import faust
import collections
import config
import alpaca_trade_api as tradeapi
import numpy as np
from lib import data, environ
from collections import namedtuple

Prices = collections.namedtuple('Prices', field_names=['open', 'high', 'low', 'close', 'volume'])

FAUST_BROKER_URL = "kafka://localhost:29092"

"""
----------------------------------
api keys and urls for brokerage api 
==================================
"""

key_id = config.ALPACA_API_KEY
secret_key = config.ALPACA_SECRET_KEY
base_url = 'https://paper-api.alpaca.markets'
data_url = 'https://data.alpaca.markets'
alpaca = tradeapi.REST(key_id, secret_key, 'https://paper-api.alpaca.markets', 'v2')

"""
-----------------------------------
connects to the api brokerage
===================================
"""
account_info = alpaca.get_account()
equity = float(account_info.equity)
margin_multiplier = float(account_info.multiplier)
total_buying_power = margin_multiplier * equity
print(f'Initial total buying power = {total_buying_power}')

@dataclass
class CryptoAgg(faust.Record, validation=True, serializer="json"):
    SYMBOL: str
    V: list
    O: list
    C: list
    H: list
    L: list

# faust -A faust_stock worker -l info

app = faust.App(
    "stock_trader",
    broker=FAUST_BROKER_URL,
    consumer_auto_offset_reset="latest",
    store='memory://',
    partitions=1)

stock_events = app.topic('STOCK_SLIDING_WINDOW')#, key_type=str, value_type=CryptoAgg)
stock_order = app.topic('STOCK_ORDERING_SYSTEM')

async def order_signal(response):
    #print("order signal ->", response)
    stock_symbol, side, quantity = response[0], response[1], response[2]
    #print("stock", stock_symbol, "side", side, "quantity", quantity)
    if (side == 'skip'):
        print("Market order of 0 | holding current position for " + stock_symbol + " | completed")
        return
    elif (side == 'close'):
        try:
            alpaca.close_position(stock_symbol)
            print("Closing position | " + str(quantity) + " " + stock_symbol + " " + side + " | completed." )
        except:
            print("No position to close | " + str(quantity) + " " + stock_symbol + " " + side + "." )
    else:
        if(quantity > 0):
            try:
                alpaca.submit_order(
                symbol=stock_symbol,
                qty=quantity,
                side='buy',
                type='market',
                time_in_force='day',
                )
                print("Market order of | " + str(quantity) + " " + stock_symbol + " " + side + " | completed.")
            except:
                print("Order of | " + str(quantity) + " " + stock_symbol + " " + side + " | did not go through.")
        else:
            print("Quantity is 0, order of | " + str(quantity) + " " + stock_symbol + " " + side + " | not completed.")   

@app.agent(stock_order)
async def orders(orderevents):
    orderevents.add_processor(order_signal)
    async for orders in orderevents:
        print("orders ->", orders)

@app.agent(stock_events)
async def stocks(stockevents):
    async for stockevent in stockevents:
        #print(f"-> Sending observation {stockevent}")
        prices = Prices(
            open=np.array(stockevent['O'], dtype=np.float32),
            high=np.array(stockevent['H'], dtype=np.float32),
            low=np.array(stockevent['L'], dtype=np.float32),
            close=np.array(stockevent['C'], dtype=np.float32),
            volume=np.array(stockevent['V'], dtype=np.float32)
            )
        #print("Worker: agent object", prices)
        prices = {stockevent['SYMBOL']: prices}
        closing_price = stockevent['PRICE'][-1]
        env = environ.StocksEnv(
            prices,
            bars_count=30,
            reset_on_close=False,
            commission=0.00,
            state_1d=False,
            random_ofs_on_reset=False,
            reward_on_close=True,
            volumes=False)
        obs = env.reset()
        #print(f"-> Sending observation {obs}")
        resp = requests.get("http://127.0.0.1:8000/trade_stocks", json={"observation": obs.tolist()}).text
        #print(f"<- Received response {stockevent['SYMBOL']}{':'}{resp}")
        #yield {stockevent['SYMBOL']: resp}
        target_value = (1 / 100) * total_buying_power

        if target_value > total_buying_power:
            target_value = total_buying_power - closing_price
        target_qty = int(target_value / closing_price)
        await stock_order.send(value=[stockevent['SYMBOL'], resp, target_qty])

if __name__ == "__main__":
    app.main()