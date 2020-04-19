import ccxt
import numpy as np
import pandas as pd

from typing import Tuple
from datetime import datetime
from collections import deque

from lib.data.providers.dates import ProviderDateFormat
from lib.data.providers import BaseDataProvider
from lib.util.helpers import format_time

from binance.enums import KLINE_INTERVAL_1MINUTE
from binance.client import Client
from binance.websockets import BinanceSocketManager

class LiveDataProvider(BaseDataProvider):
    _current_index = '2020-04-01T00:00:00Z'
    _has_loaded_historical = False

    def __init__(self,
                 exchange_name: str = 'binance',
                 symbol_pair: str = 'BTC/USDT',
                 timeframe: str = '1m',
                 start_date: datetime = None,
                 date_format: ProviderDateFormat = ProviderDateFormat.TIMESTAMP_MS,
                 data_frame: pd.DataFrame = None,
                 **kwargs):
        BaseDataProvider.__init__(self, date_format, **kwargs)

        self.exchange_name = exchange_name
        self.symbol_pair = symbol_pair
        self.timeframe = timeframe
        self.data_frame = data_frame
        self.start_date = start_date

        self.kwargs = kwargs
        
        self.kline_stream = deque(maxlen=10)
        api_key = ''
        api_secret = ''
        self.client = Client(api_key, api_secret)
        self.bm = BinanceSocketManager(self.client)
        # start any sockets here, i.e a trade socket
        pr = lambda x: print("msg received")
        self.conn_key = self.bm.start_kline_socket('BTCUSDT', self.process_message, interval=KLINE_INTERVAL_1MINUTE)
        # then start the socket manager
        self.bm.start()

        get_exchange_fn = getattr(ccxt, self.exchange_name)

        try:
            self.exchange = get_exchange_fn()
        except AttributeError:
            raise ModuleNotFoundError(
                f'Exchange {self.exchange_name} not found. Please check if the exchange is supported.')

        if not self.exchange.has['fetchOHLCV']:
            raise AttributeError(
                f'Exchange {self.exchange_name} does not support fetchOHLCV')        

    def process_message(self, msg):
        if msg['e'] == 'error':
            # close and restart the socket
            print('Socket error')
            self.close_socket()
        else:
            # process message normally
            filtered_msg = {}
            cols = {'Date': 't', 'Open': 'o', 'High': 'h',
                'Low': 'l', 'Close': 'c', 'Volume': 'v'}            
            for key, value in cols.items():
                filtered_msg[key] = msg['k'][value]
            self.kline_stream.append(filtered_msg)

    def close_socket(self):
        self.bm.stop_socket(self.conn_key)

    def has_next_ohlcv(self) -> bool:
        return True

    def reset_ohlcv_index(self, index: datetime = '2020-04-01T00:00:00Z'):
        self._current_index = index

    def next_ohlcv(self) -> pd.DataFrame:
        if len(self.kline_stream):
            latest_kline = self.kline_stream[-1]
            # self._current_index = format_time(latest_kline['Date'])
            frame = pd.DataFrame([latest_kline], columns=self.columns, dtype="float64")
            frame = self.prepare_data(frame)
            if self.data_frame is None:
                self.data_frame = pd.DataFrame(None, columns=self.columns)
            frame.astype({
                    'Date': 'int64',
                    'Open': 'float64',
                    'High': 'float64',
                    'Low': 'float64',
                    'Close': 'float64',
                    'Volume': 'float64',             
            })
            self.data_frame = self.data_frame.append(frame, ignore_index=True)
            r, c = self.data_frame.shape
            return frame
        return None

    def historical_ohlcv(self):
        return 

    def split_data_train_test(self):
        return