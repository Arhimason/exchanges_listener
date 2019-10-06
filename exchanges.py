import json
import logging
import sys
import traceback
import zlib

import aiohttp


def exceptions_watcher(func):
    async def f(self):
        try:
            await func(self)
        except:
            exc_info = sys.exc_info()
            exc_info = str(exc_info[0]) + ', ' + str(exc_info[1]) + '\n\nTraceback:\n' + ''.join(
                traceback.format_tb(exc_info[2]))
            print(exc_info)

    return f


class AbstractExchangeListener:
    name = None

    def __init__(self, currency_pairs, on_changed=None):
        self.storage = {}
        self.currency_pairs = currency_pairs
        self.on_changed = on_changed

        for currency_pair in currency_pairs:
            self.storage[currency_pair] = {'bid_prices': {}, 'ask_prices': {},
                                           'max_bid': None, 'min_ask': None}

    def _encode_currency_pair(self, currency_pair):
        return currency_pair

    def _decode_currency_pair(self, currency_pair):
        return currency_pair

    def _on_update(self, currency_pair):
        pass

    async def worker(self):
        pass


class SimpleExchangeListener(AbstractExchangeListener):

    async def _on_update(self, currency_pair):
        storage = self.storage[currency_pair]
        max_bid = 0
        for price, quantity in storage['bid_prices'].items():
            if quantity == 0: continue

            if price > max_bid:
                max_bid = price

        min_ask = 0
        for price, quantity in storage['ask_prices'].items():
            if quantity == 0: continue

            if price < min_ask:
                min_ask = price

        if storage['max_bid'] != max_bid or storage['min_ask'] != min_ask:
            storage['max_bid'] = max_bid
            storage['min_ask'] = min_ask

            if self.on_changed:
                await self.on_changed(self, max_bid, min_ask)


class BinanceListener(SimpleExchangeListener):
    name = 'binance'

    def _encode_currency_pair(self, currency_pair):
        return currency_pair.lower().replace("/", "")

    def _decode_currency_pair(self, currency_pair):
        currency_pair = currency_pair.upper()
        return currency_pair[:3] + '/' + currency_pair[3:]

    @exceptions_watcher
    async def worker(self):
        streams = [f'{self._encode_currency_pair(d)}@depth.b10' for d in self.currency_pairs]
        streams_str = '/'.join(streams)
        url = f'https://stream.binance.com:9443/stream?streams={streams_str}'
        session = aiohttp.ClientSession()
        ws = await session.ws_connect(url)

        while True:
            msg = await ws.receive()

            if msg.type == aiohttp.WSMsgType.text:
                data = json.loads(msg.data)
                logging.debug(data)
                data = data['data']

                currency_pair = self._decode_currency_pair(data['s'])
                storage = self.storage[currency_pair]

                for b in data['b']:
                    b = list(map(float, b))
                    storage['bid_prices'][b[0]] = b[1]

                for s in data['a']:
                    s = list(map(float, s))
                    storage['ask_prices'][s[0]] = s[1]

                await self._on_update(currency_pair)
            elif msg.type == aiohttp.WSMsgType.closed:
                # todo handle closed
                break
            elif msg.type == aiohttp.WSMsgType.error:
                # todo handle error
                break


class OkexListener(SimpleExchangeListener):
    name = 'okex'

    def _encode_currency_pair(self, currency_pair):
        return currency_pair.replace("/", "-")

    def _decode_currency_pair(self, currency_pair):
        return currency_pair.replace("-", "/")

    @exceptions_watcher
    async def worker(self):
        streams = [f'spot/optimized_depth:{self._encode_currency_pair(d)}' for d in self.currency_pairs]
        url = 'https://okexcomreal.bafang.com:8443/ws/v3'
        session = aiohttp.ClientSession()
        ws = await session.ws_connect(url)

        payload = {"op": "subscribe",
                   "args": streams
                   }

        await ws.send_str(json.dumps(payload))

        while True:
            msg = await ws.receive()

            if msg.type == aiohttp.WSMsgType.BINARY:
                data = zlib.decompress(msg.data, -15)
                data = json.loads(data)
                logging.debug(data)

                if 'table' not in data:
                    continue

                data = data['data'][0]
                currency_pair = self._decode_currency_pair(data['instrument_id'])
                storage = self.storage[currency_pair]

                for b in data['bids']:
                    b = list(map(float, b))
                    storage['bid_prices'][b[0]] = b[1]

                for s in data['asks']:
                    s = list(map(float, s))
                    storage['ask_prices'][s[0]] = s[1]

                await self._on_update(currency_pair)
            elif msg.type == aiohttp.WSMsgType.closed:
                # todo handle closed
                break
            elif msg.type == aiohttp.WSMsgType.error:
                # todo handle error
                break
