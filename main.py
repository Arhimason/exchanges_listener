import asyncio
import logging
import time

from exchanges import BinanceListener, OkexListener

logging.basicConfig(level=logging.INFO)

currency_pairs = ['BTC/USDT']
exchanges = [BinanceListener, OkexListener]


async def on_changed(exchange, max_bid, min_ask):
    print(time.time(), exchange.name, (max_bid + min_ask)/2)


async def main():
    tasks = []
    for exchange in exchanges:
        exchange = exchange(currency_pairs, on_changed=on_changed)
        task = asyncio.ensure_future(exchange.worker())
        tasks.append(task)
    await asyncio.gather(*tasks)


loop = asyncio.get_event_loop()
future = asyncio.ensure_future(main())
loop.run_forever()
