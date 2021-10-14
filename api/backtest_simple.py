import sys
sys.path.append("../shared")
sys.path.append("")
import logging
import numpy as np
import df_utils
import file_utils
import time
import pandas as pd
from plotly.subplots import make_subplots
from plotly.offline import plot
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
logger = logging.getLogger(__name__)

NONE = 0
LONG = 1
SHORT = -1
DEBUG = False
DEVIATION_POW = 4

if DEBUG:
    pass

"""
███████╗████████╗ █████╗ ████████╗██╗███████╗████████╗██╗ ██████╗███████╗
██╔════╝╚══██╔══╝██╔══██╗╚══██╔══╝██║██╔════╝╚══██╔══╝██║██╔════╝██╔════╝
███████╗   ██║   ███████║   ██║   ██║███████╗   ██║   ██║██║     ███████╗
╚════██║   ██║   ██╔══██║   ██║   ██║╚════██║   ██║   ██║██║     ╚════██║
███████║   ██║   ██║  ██║   ██║   ██║███████║   ██║   ██║╚██████╗███████║
╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝╚══════╝   ╚═╝   ╚═╝ ╚═════╝╚══════╝
"""



def _sortino_ratio(returns: pd.Series) -> float:
    """Return the sortino ratio for a given series of a returns.

    References:
        - https://en.wikipedia.org/wiki/Sortino_ratio
    """
    _target_returns = 0
    _risk_free_rate = 0

    downside_returns = returns.copy()
    downside_returns[returns < _target_returns] = returns ** 2

    expected_return = np.mean(returns)
    downside_std = np.sqrt(np.std(downside_returns))

    return (expected_return - _risk_free_rate + 1E-9) / (downside_std + 1E-9)

def _sortino_ratio_v2(returns) -> float:
    """Return the sortino ratio for a given series of a returns.

    References:
        - https://en.wikipedia.org/wiki/Sortino_ratio
    """
    _target_returns = 0
    _risk_free_rate = 0

    #downside_returns = np.copy(returns)
    downside_returns = np.where(returns < _target_returns, returns ** 2, returns)

    expected_return = np.mean(returns)
    downside_std = np.sqrt(np.std(downside_returns))

    return (expected_return - _risk_free_rate + 1E-9) / (downside_std + 1E-9)

def _sharpe_ratio(self, returns: pd.Series) -> float:
    """Return the sharpe ratio for a given series of a returns.

    References:
        - https://en.wikipedia.org/wiki/Sharpe_ratio
    """
    return (np.mean(returns) - self._risk_free_rate + 1E-9) / (np.std(returns) + 1E-9)


def _sharpe_ratio_v2(returns) -> float:
    _target_returns = 0
    _risk_free_rate = 0
    """Return the sharpe ratio for a given series of a returns.

    References:
        - https://en.wikipedia.org/wiki/Sharpe_ratio
    """
    return (np.mean(returns) - _risk_free_rate + 1E-9) / (np.std(returns) + 1E-9)



def linreg(list, forward=1):
    regression_model = LinearRegression()
    # print(row)
    index = np.arange(len(list))
    pred = np.arange(len(list) + forward)
    row = np.array(list)

    regression_model.fit(index.reshape(-1, 1), row.reshape(-1, 1))
    val = regression_model.predict(pred.reshape(-1, 1))[-1]
    return val.item()

def linreg_np(list, forward=1):
    regression_model = LinearRegression()
    # print(row)
    index = np.arange(len(list))
    pred = np.arange(len(list) + forward)
    row = list

    regression_model.fit(index.reshape(-1, 1), row.reshape(-1, 1))
    val = regression_model.predict(pred.reshape(-1, 1))[-1]
    return val.item()



"""
████████╗██████╗  █████╗ ██████╗ ███████╗
╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝
   ██║   ██████╔╝███████║██║  ██║█████╗  
   ██║   ██╔══██╗██╔══██║██║  ██║██╔══╝  
   ██║   ██║  ██║██║  ██║██████╔╝███████╗
   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝
"""


class Trade():
    def __init__(self, id, position, open_time, open_rate, open_fee, open_fee_pct, i):
        self.id = id
        self.position = position
        self.open_i = i
        self.close_i = 0
        self.open_time = open_time
        self.open_rate = open_rate
        self.open_fee = open_fee
        self.open_fee_pct = open_fee_pct
        self.close_time = None
        self.close_rate = None
        self.close_fee = None
        self.profit_pct = None
        self.profit_pip = None
        self.duration = None
        self.duration_candles = 0

    def close(self, close_time, close_rate, close_fee, close_fee_pct, pip_multiplier, i):
        self.close_time = close_time
        self.close_rate = close_rate
        self.close_fee = close_fee
        self.close_fee_pct = close_fee_pct
        self.close_i = i

        open_fee = (self.open_rate * self.open_fee_pct) + (self.open_fee)
        close_fee = (self.close_rate * self.close_fee_pct) + (self.close_fee)

        if self.position == LONG:
            self.profit_pct = ((self.close_rate - close_fee) / (self.open_rate + open_fee)) - 1
            self.profit_pip = ((self.close_rate - close_fee) - (self.open_rate + open_fee))
        elif self.position == SHORT:
            self.profit_pct = ((self.open_rate - open_fee) / (self.close_rate + open_fee)) - 1
            self.profit_pip = ((self.open_rate - open_fee) - (self.close_rate + open_fee))

        self.profit_pip = int(self.profit_pip * pip_multiplier)
        try:
            self.duration = int((self.close_time - self.open_time).total_seconds() / 60)
        except:
            pass
        self.duration_candles = self.close_i - self.open_i


    def to_dict(self):
        return {
            "id": self.id,
            "position": self.position,
            "open_time": self.open_time,
            "open_rate": self.open_rate,
            "open_fee": self.open_fee,
            "close_time": self.close_time,
            "close_rate": self.close_rate,
            "close_fee": self.close_fee,
            "profit_pct": self.profit_pct,
            "profit_pip": self.profit_pip,
            "duration": self.duration,
            "duration_candles": self.duration_candles,
            "open_i": self.open_i,
            "close_i": self.close_i
        }

class BacktestSimpleResult(Base):
    __tablename__ = 'results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pair_id = Column(Integer)
    seed = Column(String)
    statement = Column(String)
    trades = Column(Integer)

    cum_profit = Column(Float)

    profit_drawdown = Column(Float)
    equity_drawdown = Column(Float)

    profit_deviation = Column(Float)
    profit_deviation_max = Column(Float)

    equity_deviation = Column(Float)
    equity_deviation_max = Column(Float)

    negative_equity = Column(Float)

    trade_duration_mean = Column(Float)
    trade_duration_median = Column(Float)
    trade_duration_variance = Column(Float)

    win_rate = Column(Float)
    cnt_wins = Column(Float)
    cnt_losses = Column(Float)

    sharpe_ratio = Column(Float)
    sortino_ratio = Column(Float)

    lr1 = Column(Float)
    lr2 = Column(Float)
    lr3 = Column(Float)
    lrk1 = Column(Float)
    lrk2 = Column(Float)
    lrk3 = Column(Float)

    buy_sell_signal_ratio = Column(Float)
    score = Column(Float)

    def __init__(self):
        self.pair_id = 0
        self.trades = 0
        self.cum_profit = 0
        self.profit_drawdown = 0
        self.equity_drawdown = 0
        self.profit_deviation = 0
        self.profit_deviation_max = 0
        self.equity_deviation = 0
        self.equity_deviation_max = 0
        self.negative_equity = 0
        self.trade_duration_mean = 0
        self.trade_duration_median = 0
        self.trade_duration_variance = 0
        self.win_rate = 0
        self.cnt_wins = 0
        self.cnt_losses = 0
        self.sharpe_ratio = 0
        self.sortino_ratio = 0
        self.lr1 = 0
        self.lr2 = 0
        self.lr3 = 0
        self.lrk1 = 0
        self.lrk2 = 0
        self.lrk3 = 0
        self.buy_sell_signal_ratio = 0
        self.score = 0


class SimpleBacktest():
    def __init__(self, df, spread=0, fee_pct=0, ping_pong=False, pip_multiplier=1):
        self.spread = spread
        self.fee_pct = fee_pct
        self.ping_pong = ping_pong
        self.pip_multiplier = pip_multiplier
        self.df = df

        tmp = df[['date', 'open', 'high', 'low', 'close', 'buy', 'sell', 'sbuy', 'ssell']].copy()
        self.date = tmp.columns.get_loc("date")
        self.open = tmp.columns.get_loc("open")
        self.high = tmp.columns.get_loc("high")
        self.low = tmp.columns.get_loc("low")
        self.close = tmp.columns.get_loc("close")
        self.buy = tmp.columns.get_loc("buy")
        self.sell = tmp.columns.get_loc("sell")
        self.sbuy = tmp.columns.get_loc("sbuy")
        self.ssell = tmp.columns.get_loc("ssell")
        self.numpy_df = tmp.to_numpy()
        self.numpy_df[:, self.buy] = 0
        self.numpy_df[:, self.sell] = 0
        self.numpy_df[:, self.sbuy] = 0
        self.numpy_df[:, self.ssell] = 0
        self.stats = BacktestSimpleResult()

        self.reset()

    def reset(self):
        # INIT
        self.i = 0
        self.trades = []
        self.cur_trade = None
        self.row = None

        self.std_duration = []
        self.std_duration_candles = []

        # Array mit Profit in % von jedem Trade
        self.std_returns = []
        # Array mit Zeit für Plotting
        self.std_df_time = []
        # Gesamtprofit bei jeder Candle
        self.std_df_profit = []
        # UPNL bei jeder Candle
        self.std_df_upnl = []
        # Equity bei jeder Candle
        self.std_df_equity = []

        self.stats = BacktestSimpleResult()

        # COPY
        self.numpy_df[:, self.buy] = 0
        self.numpy_df[:, self.sell] = 0
        self.numpy_df[:, self.sbuy] = 0
        self.numpy_df[:, self.ssell] = 0


    def _open(self, position):
        self.cur_trade = Trade(
            id=len(self.trades),
            position=position,
            open_time=self.row[self.date],
            open_rate=self.row[self.open],
            open_fee=self.spread,
            open_fee_pct=self.fee_pct,
            i=self.i
        )
        self.trades.append(self.cur_trade)

    def _close(self):
        self.cur_trade.close(
            close_time=self.row[self.date],
            close_rate=self.row[self.open],
            close_fee=self.spread,
            close_fee_pct=self.fee_pct,
            pip_multiplier=self.pip_multiplier,
            i=self.i
        )
        self.stats.trades += 1
        self.stats.cum_profit = self.stats.cum_profit + self.cur_trade.profit_pct
        #self.stats.cum_profit_pip = self.stats.cum_profit_pip + self.cur_trade.profit_pip

        if self.cur_trade.profit_pct > 0:
            self.stats.cnt_wins += 1
        else:
            self.stats.cnt_losses += 1

        self.std_returns.append(self.cur_trade.profit_pct)
        self.std_duration.append(self.cur_trade.duration)
        self.std_duration_candles.append(self.cur_trade.duration_candles)

        self.cur_trade = None


    def _get_upnl(self, trade, close_rate):
        open_fee = (trade.open_rate * trade.open_fee_pct) + (trade.open_fee)
        close_fee = (close_rate * trade.open_fee_pct) + (trade.open_fee)

        if trade.position == LONG:
            profit_pct = ((close_rate - close_fee) / (trade.open_rate + open_fee)) - 1
        elif trade.position == SHORT:
            profit_pct = ((trade.open_rate - open_fee) / (close_rate + open_fee)) - 1

        return profit_pct


    def backtest(self, np_buy: np.ndarray, np_sell: np.ndarray, start: int=None, end: int=None, np_sbuy: np.ndarray=None, np_ssell: np.ndarray=None):
        #date = self.date
        #open = self.open
        #high = self.high
        #low = self.low
        #close = self.close
        buy = self.buy
        sell = self.sell
        sbuy = self.sbuy
        ssell = self.ssell
        df = self.numpy_df

        df[:, buy] = np_buy[:]
        df[:, sell] = np_sell[:]

        if self.ping_pong:
            df[:, sbuy] = np_buy[:]
            df[:, ssell] = np_sell[:]
        else:
            if np_sbuy is not None:
                df[:, sbuy] = np_sbuy[:]
            else:
                df[:, sbuy] = 0

            if np_ssell is not None:
                df[:, ssell] = np_ssell[:]
            else:
                df[:, ssell] = 0

        # Buy/Sell Ratio
        buys = np.sum(np_buy)
        sells = np.sum(np_sell)
        if buys > 1 and sells > 1:
            if buys > sells:
                self.stats.buy_sell_signal_ratio = sells / buys
            else:
                self.stats.buy_sell_signal_ratio = buys / sells
        else:
            self.stats = None
            return False

        length = df.shape[0]
        if start is None:
            start = 0
        if end is None:
            end = length - 1

        last_upnl = 0
        equity = 0
        for i in range(start, end):
            rrow = df[i]
            self.row = rrow
            self.i = i

            new_equity = equity
            upnl = 0

            if i > 0:
                lrow = df[i - 1]  # lrow nehmen damit automatisch geshiftet wird
                # Kein Trade -> Open
                if self.cur_trade is None:
                    # Open
                    if lrow[buy] == 1:
                        self._open(LONG)
                    elif lrow[ssell] == 1:
                        self._open(SHORT)
                # Trade -> Close oder Close und neuer Open
                else:
                    # UPNL holen für Equity
                    upnl = self._get_upnl(self.cur_trade, self.row[self.open])
                    new_equity = equity + upnl

                    if lrow[sell] == 1 and lrow[buy] == 1:
                        raise Exception("Kann nicht Long-kaufen und Long-verkaufen!")
                    if lrow[ssell] == 1 and lrow[sbuy] == 1:
                        raise Exception("Kann nicht Short-verkaufen und Short-kaufen!")
                    if lrow[ssell] == 1 and lrow[buy] == 1:
                        raise Exception("Kann nicht Short-verkaufen und Long-kaufen!")

                    # Close-Signal
                    if (lrow[sell] == 1 or lrow[ssell] == 1) and self.cur_trade.position == LONG:
                        self._close()
                        equity = self.stats.cum_profit
                    elif (lrow[buy] == 1 or lrow[sbuy] == 1) and self.cur_trade.position == SHORT:
                        self._close()
                        equity = self.stats.cum_profit

                    if self.cur_trade is None:
                        if lrow[buy] == 1:
                            self._open(LONG)
                        if lrow[ssell] == 1:
                            self._open(SHORT)

            self.std_df_time.append(self.row[self.date])
            self.std_df_profit.append(self.stats.cum_profit)
            self.std_df_upnl.append(upnl)
            self.std_df_equity.append(new_equity)

        if self.cur_trade is not None:
            self.i += 1
            rrow = df[i]
            self.row = rrow
            self._close()
            new_equity = self.stats.cum_profit
            upnl = 0

        # Statistik vorbereiten
        self.stats.trades_cnt = len(self.trades)
        if (self.stats.trades_cnt < 3) or (self.stats.cum_profit < 0):
            logger.debug("Keine Trades oder kein Gewinn!")
            self.stats = None
            return False

        if self.row is not None:
            self.std_df_time.append(self.row[self.date])
            self.std_df_profit.append(self.stats.cum_profit)
            self.std_df_upnl.append(upnl)
            self.std_df_equity.append(new_equity)


        assert(len(self.std_df_equity) == len(np_buy))

        self.get_stats_np()
        return True

    """
     █████╗ ██╗   ██╗███████╗██╗    ██╗███████╗██████╗ ████████╗███████╗███╗   ██╗
    ██╔══██╗██║   ██║██╔════╝██║    ██║██╔════╝██╔══██╗╚══██╔══╝██╔════╝████╗  ██║
    ███████║██║   ██║███████╗██║ █╗ ██║█████╗  ██████╔╝   ██║   █████╗  ██╔██╗ ██║
    ██╔══██║██║   ██║╚════██║██║███╗██║██╔══╝  ██╔══██╗   ██║   ██╔══╝  ██║╚██╗██║
    ██║  ██║╚██████╔╝███████║╚███╔███╔╝███████╗██║  ██║   ██║   ███████╗██║ ╚████║
    ╚═╝  ╚═╝ ╚═════╝ ╚══════╝ ╚══╝╚══╝ ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═══╝
    """


    def get_stats_np(self):
        if self.stats.cnt_losses <= 0:
            self.stats.cnt_losses = 1
        if self.stats.cnt_wins <= 0:
            self.stats.cnt_wins = 1
        self.stats.win_rate = self.stats.cnt_wins / self.stats.cnt_losses

        profit = np.array(self.std_df_profit)
        equity = np.array(self.std_df_equity)
        upnl = np.array(self.std_df_upnl)

        equity_norm = df_utils.normalize_np(equity)
        profit_norm = df_utils.normalize_np(profit)
        #upnl_norm = df_utils.normalize_np(upnl)

        self.stats.equity = equity_norm
        self.stats.profit = profit_norm

        # Profit Drawdown PCT
        min_right = np.minimum.accumulate(profit_norm[::-1])[::-1]
        self.stats.profit_drawdown = np.max(np.abs(profit_norm - min_right))

        # Equity Drawdown PCT
        min_right = np.minimum.accumulate(equity_norm[::-1])[::-1]
        self.stats.equity_drawdown = np.max(np.abs(equity_norm - min_right))        

        # Profit Deviation PCT
        step = 1 / len(profit_norm)
        perfekte_gewinnkurve = np.full(len(profit_norm), step).cumsum()
        deviation = np.abs(profit_norm - perfekte_gewinnkurve)
        self.stats.profit_deviation = np.sum(deviation) / len(profit_norm)
        self.stats.profit_deviation_max = np.max(deviation)

        # Equity Deviation PCT
        step = 1 / len(equity_norm)
        perfekte_gewinnkurve = np.full(len(equity_norm), step).cumsum()
        deviation = np.abs(equity_norm - perfekte_gewinnkurve)
        self.stats.equity_deviation = np.sum(deviation) / len(equity_norm)
        self.stats.equity_deviation_max = np.max(deviation)

        # Avg, Median Trade Duration
        duration_candles = np.array(self.std_duration_candles)
        self.stats.trade_duration_mean = np.mean(duration_candles)
        self.stats.trade_duration_median = np.median(duration_candles)
        self.stats.trade_duration_variance = np.var(duration_candles, ddof=1)

        # Negative Equity
        self.stats.negative_equity = np.sum(np.power(1 + np.abs(np.where(upnl > 0, 0, upnl)), 6))

        # Sortino
        returns = np.array(self.std_returns)    
        ratio = _sortino_ratio_v2(returns)
        if ratio > 100:
            ratio = -1
        self.stats.sortino_ratio = ratio

        # Sharpe
        ratio = _sharpe_ratio_v2(returns)
        if ratio > 100:
            ratio = -1
        self.stats.sharpe_ratio = ratio

        # Linreg
        y = profit_norm
        length = len(y)

        l1 = int(length) - 1
        l2 = int(length / 2)
        l3 = int(length / 3)

        self.stats.lr1 = 1
        self.stats.lr2 = 1
        self.stats.lr3 = 1
        self.stats.lrk3 = 1
        self.stats.lrk2 = 1
        self.stats.lrk1 = 1
        """
        self.stats.lr1 = linreg_np(y[-l1:])
        self.stats.lr2 = linreg_np(y[-l2:])
        self.stats.lr3 = linreg_np(y[-l3:])

        self.stats.lrk3 = linreg_np(y[-l3:], 2) - linreg_np(y[-l3:], 1)
        self.stats.lrk2 = linreg_np(y[-l2:], 2) - linreg_np(y[-l2:], 1)
        self.stats.lrk1 = linreg_np(y[-l1:], 2) - linreg_np(y[-l1:], 1)
        """


    """
    ██████╗ ██╗      ██████╗ ████████╗████████╗██╗███╗   ██╗ ██████╗ 
    ██╔══██╗██║     ██╔═══██╗╚══██╔══╝╚══██╔══╝██║████╗  ██║██╔════╝ 
    ██████╔╝██║     ██║   ██║   ██║      ██║   ██║██╔██╗ ██║██║  ███╗
    ██╔═══╝ ██║     ██║   ██║   ██║      ██║   ██║██║╚██╗██║██║   ██║
    ██║     ███████╗╚██████╔╝   ██║      ██║   ██║██║ ╚████║╚██████╔╝
    ╚═╝     ╚══════╝ ╚═════╝    ╚═╝      ╚═╝   ╚═╝╚═╝  ╚═══╝ ╚═════╝ 
    """



    def plot_plotly(self, name, path, indicators=[]):
        self.indicators = indicators
        fig = self.generate_candlestick_graph(
            pair=name,
            data=self.df
        )

        trades = self.get_trades()
        fig = self.plot_trades(fig, trades)
        fig = self.plot_profit(fig)

        fig.update_layout(showlegend=True)
        fig.update_yaxes(automargin=True)
        fig.update_xaxes(automargin=True)
        fig.update_layout(
            autosize=True,
            margin=go.layout.Margin(
                l=0,
                r=0,
                b=0,
                t=30,
                pad=0
            )
        )

        plot(fig, filename=path, auto_open=False)


    def get_trades(self):
        self.df_trades = pd.DataFrame.from_records([s.to_dict() for s in self.trades])
        if len(self.df_trades) > 0:
            self.df_trades["profit_pct"] = self.df_trades["profit_pct"] * 100
            self.profit = self.df_trades["profit_pct"].sum()
        else:
            self.profit = -1
        return self.df_trades


    def generate_candlestick_graph(self, pair: str, data: pd.DataFrame) -> go.Figure:
        # Define the graph
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            row_width=[1, 1],
            vertical_spacing=0,
        )
        fig['layout'].update(title=pair)
        fig['layout']['yaxis1'].update(title='Price')
        fig['layout']['yaxis2'].update(title='Balance')

        #fig['layout']['yaxis3'].update(title='Other')
        fig['layout']['xaxis']['rangeslider'].update(visible=False)
        #fig['layout']['xaxis'].update(type=False)

        if len(data.index) > 1024:
            # Common information
            candles = go.Candlestick(
                x=data.date,
                open=data.open,
                high=data.high,
                low=data.low,
                close=data.close,
                name='Price'
                #text=data.status
            )
        else:
            candles = go.Scatter(
            x=data.date,
            y=data.close,
            name='Price',
            fillcolor="black"
            )
        fig.add_trace(candles, 1, 1)

        for indi in self.indicators:
            if indi in data.columns:
                candles = go.Scatter(
                    x=data.date,
                    y=data[indi],
                    name=indi,
                    fillcolor="blue"
                )
                fig.add_trace(candles, 1, 1)


        op = 0.5
        size = 11
        width = 2

        if 'buy' in data.columns:
            df_buy = data[data['buy'] == 1]
            if len(df_buy) > 0:
                buys = go.Scatter(
                    x=df_buy.date,
                    y=df_buy.close,
                    mode='markers',
                    name='buy',
                    opacity=op,
                    marker=dict(
                        symbol='triangle-up-open',
                        size=size,
                        line=dict(width=width),
                        color='green',
                    )
                )
                fig.add_trace(buys, 1, 1)

        if 'sell' in data.columns:
            df_sell = data[data['sell'] == 1]
            if len(df_sell) > 0:
                sells = go.Scatter(
                    x=df_sell.date,
                    y=df_sell.close,
                    mode='markers',
                    name='sell',
                    opacity=op,
                    marker=dict(
                        symbol='circle-open',
                        size=size,
                        line=dict(width=width),
                        color='red',
                    )
                )
                fig.add_trace(sells, 1, 1)


        if 'sbuy' in data.columns:
            df_buy = data[data['sbuy'] == 1]
            if len(df_buy) > 0:
                buys = go.Scatter(
                    x=df_buy.date,
                    y=df_buy.close,
                    mode='markers',
                    name='sbuy',
                    opacity=op,
                    marker=dict(
                        symbol='circle-open',
                        size=size,
                        line=dict(width=width),
                        color='cyan',
                    )
                )
                fig.add_trace(buys, 1, 1)


        if 'ssell' in data.columns:
            df_sell = data[data['ssell'] == 1]
            if len(df_sell) > 0:
                sells = go.Scatter(
                    x=df_sell.date,
                    y=df_sell.close,
                    mode='markers',
                    name='ssell',
                    opacity=op,
                    marker=dict(
                        symbol='triangle-down-open',
                        size=size,
                        line=dict(width=width),
                        color='orange',
                    )
                )
                fig.add_trace(sells, 1, 1)

        return fig

    def plot_profit(self, fig) -> make_subplots:
        profit = go.Scatter(
            x=self.std_df_time,
            y=self.std_df_profit,
            name='Cum Profit'
        )
        fig.add_trace(profit, 2, 1)

        profit = go.Scatter(
            x=self.std_df_time,
            y=self.std_df_equity,
            name='Equity'
        )
        fig.add_trace(profit, 2, 1)

        profit = go.Scatter(
            x=self.std_df_time,
            y=self.std_df_upnl,
            name='UPNL'
        )
        fig.add_trace(profit, 2, 1)

        return fig


    def plot_trades(self, fig, trades: pd.DataFrame) -> make_subplots:
        # Trades can be empty
        if trades is not None and len(trades) > 0:
            longs = trades[trades["position"] == 1]
            shorts = trades[trades["position"] == -1]

            def tmp(df, name):
                if len(df.index) > 0:
                    color_entry = df.apply(lambda row: 'green' if row['position'] == 1 else 'orange', axis=1)
                    color_exit = df.apply(lambda row: 'red' if row['position'] == 1 else 'cyan', axis=1)
                    shape = df.apply(lambda row: 'square-open' if row['position'] == 1 else 'diamond-open', axis=1)

                    trade_buys = go.Scatter(
                        x=df["open_time"],
                        y=df["open_rate"],
                        mode='markers',
                        name=name + " OPEN",
                        marker=dict(
                            symbol=shape,
                            size=11,
                            line=dict(width=2),
                            color=color_entry
                        )
                    )
                    desc = df.apply(lambda row: f"{round((row['profit_pct']), 3)}%,"
                                                    f"{row['duration']}min",
                                        axis=1)
                    trade_sells = go.Scatter(
                        x=df["close_time"],
                        y=df["close_rate"],
                        text=desc,
                        mode='markers',
                        name=name + " CLOSE",
                        marker=dict(
                            symbol=shape,
                            size=11,
                            line=dict(width=2),
                            color=color_exit
                        )
                    )
                    fig.add_trace(trade_buys, 1, 1)
                    fig.add_trace(trade_sells, 1, 1)

            tmp(longs, "LONG")
            tmp(shorts, "SHORT")
        else:
            logger.warning("No trades found.")
        return fig