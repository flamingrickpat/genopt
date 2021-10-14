import sys
sys.path.append("../shared")
sys.path.append("")

import pandas as pd
import numpy as np
import file_utils
import time
import logging


logger = logging.getLogger(__name__)

class VectorizedBacktest():
    def __init__(self, df, spread=0.0001, fee_pct=0, ping_pong=False):
        self.spread = spread
        self.fee_pct = fee_pct
        self.ping_pong = ping_pong
        self.df = df
        self.equity = None

    def reset(self):
        self.equity = None

    def backtest(self, np_buy, np_sell):
        prices = self.df["close"]
        np_singal = np.where(np_buy == 1, 1, -1)
        rs = prices.apply(np.log).diff(1)
        pos = pd.Series(np_singal)
        my_rs = pos.shift(1) * rs
        equity = np.add(-1, my_rs.cumsum().apply(np.exp))
        self.equity = equity.values