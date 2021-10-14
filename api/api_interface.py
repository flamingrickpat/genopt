import copy
import threading
import logging
import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot
from plotly.subplots import make_subplots

logger = logging.getLogger()

class ApiInterface():
    def __init__(self, api_key: str, api_secret: str, symbol: str, interval: int, stake_currency: str):
        self.symbol = symbol
        self.interval = interval
        self.stake_currency = stake_currency
        self.lock_df = threading.Lock()
        self.df_orig = pd.DataFrame()

    def init_df(self, df):
        pass

    def tick(self, i):
        pass

    def set_leverage(self, leverage):
        pass

    def get_open(self):
        return None

    def get_close(self):
        return None

    def open_order(self, type, contracts, price=0, reduce_only=False, post_only=False, max_lifetime=0, tp=None, sl=None):
        return None

    def cancel_order(self, id):
        return None

    def cancel_all_orders(self):
        return None

    def set_limit_order(self, id, limit):
        return None

    def get_position(self):
        return None

    def get_availible_balance(self):
        return None

    def get_equity(self):
        return None

    def get_upnl(self):
        return None

    def fill_initial_orders(self):
        return None

    def get_contracts_pct_of_balance(self, percentage):
        pass

    """
    ██████╗ ██╗      ██████╗ ████████╗████████╗██╗███╗   ██╗ ██████╗ 
    ██╔══██╗██║     ██╔═══██╗╚══██╔══╝╚══██╔══╝██║████╗  ██║██╔════╝ 
    ██████╔╝██║     ██║   ██║   ██║      ██║   ██║██╔██╗ ██║██║  ███╗
    ██╔═══╝ ██║     ██║   ██║   ██║      ██║   ██║██║╚██╗██║██║   ██║
    ██║     ███████╗╚██████╔╝   ██║      ██║   ██║██║ ╚████║╚██████╔╝
    ╚═╝     ╚══════╝ ╚═════╝    ╚═╝      ╚═╝   ╚═╝╚═╝  ╚═══╝ ╚═════╝ 
    """

    def _plot(self, df, trades, orders, liquidations, name, filename, indicators):
        fig = self._generate_candlestick_graph(
            pair=name,
            data=df,
            plot_indicators=indicators
        )
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

        self._plot_trades(fig, trades)
        self._plot_orders(fig, orders)
        self._plot_liquidations(fig, liquidations)
        self._plot_k(fig)
        plot(fig, filename=filename, auto_open=False)

    def _generate_candlestick_graph(self, pair: str, data: pd.DataFrame, plot_indicators=None) -> go.Figure:
        if plot_indicators is None:
            plot_indicators = []

        # Define the graph
        fig = make_subplots(
            rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.03,
            row_heights=[0.55, 0.15, 0.15, 0.15],
        )
        fig['layout'].update(title=pair)
        fig['layout']['yaxis1'].update(title='Price')
        fig['layout']['yaxis2'].update(title='Balance')
        fig['layout']['xaxis']['rangeslider'].update(visible=False)

        fig['layout']['yaxis'].update(autorange=True)
        fig['layout']['yaxis'].update(fixedrange=False)

        fig.update_xaxes(showticklabels=False)

        fig.update_xaxes(linecolor='Grey', gridcolor='Gainsboro')
        fig.update_yaxes(linecolor='Grey', gridcolor='Gainsboro')
        fig.update_xaxes(title_text='Price', row=1)
        fig.update_xaxes(title_text='Indikators', row=2)
        fig.update_xaxes(title_text='Position', row=3)
        fig.update_xaxes(title_text='Profit', row=4)
        fig.update_xaxes(title_standoff=7, title_font=dict(size=12))

        # Price
        candles = go.Candlestick(
            x=data["date_plot"],
            open=data.open,
            high=data.high,
            low=data.low,
            close=data.close,
            name='Price Candlestick',
            visible="legendonly"
        )
        fig.add_trace(candles, 1, 1)

        candles = go.Scatter(
            x=data["date_plot"],
            y=data["close"],
            name='Price',
            line={'color': "black"},
            visible=True
        )
        fig.add_trace(candles, 1, 1)

        # Volume
        volume = go.Bar(
            x=data["date_plot"],
            y=data['volume'],
            name='Volume',
            marker_color='DodgerBlue',
            marker_line_color='DodgerBlue',
            visible="legendonly"
        )
        fig.add_trace(volume, 1, 1)

        """
        self.df_orig["margin_balance"] = np.nan # Margin Balance = Basiswert + UPNL
        self.df_orig["account_balance"] = np.nan # Basiswert = Deposit + total RPNL
        self.df_orig["upnl"] = np.nan
        self.df_orig["upnlp"] = np.nan
        self.df_orig["rpnl"] = np.nan
        self.df_orig["available_balance"] = np.nan # Margin Balance - Order Margin - Position Margin
        self.df_orig["position_margin"] = np.nan # Minimum Equity to hold Position. (Entry Value of all contracts / Leverage) + UPNL
        self.df_orig["order_margin"] = np.nan# Minimum Equity to keep Order. (Order Value / Leverage)
        """

        def add_scatter(column, visible, x):
            if visible:
                v = True
            else:
                v = "legendonly"
            profit = go.Scatter(
                x=data["date_plot"],
                y=data[column],
                name=column,
                visible=v
            )
            fig.add_trace(profit, x, 1)

        add_scatter("position", True, 3)
        add_scatter("margin_balance", False, 4)
        add_scatter("account_balance", False, 4)
        add_scatter("upnl", True, 4)
        add_scatter("upnlp", False, 4)
        add_scatter("rpnl", True, 4)
        add_scatter("available_balance", False, 4)
        add_scatter("position_margin", False, 4)
        add_scatter("order_margin", False, 4)

        # Indicators
        tmp = copy.copy(plot_indicators)
        tmp.append(
            {
                "plot": True,
                "name": "average_entry_price",
                "overlay": True,
                "scatter": False,
                "color": "blue",
                "visible": True
            })
        tmp.append(
            {
                "plot": True,
                "name": "liquidation_price",
                "overlay": True,
                "scatter": False,
                "color": "red",
                "visible": True
            }
        )
        for indicator in tmp:
            name = indicator["name"]
            plot = True
            if "plot" in indicator and indicator["plot"]:
                plot = indicator["plot"]

            if name in data and plot:
                visible = "legendonly"
                if "visible" in indicator:
                    if indicator["visible"]:
                        visible = True

                mode = "lines"
                if "scatter" in indicator and indicator["scatter"]:
                    mode = "markers"

                fillcolor = None
                if "color" in indicator:
                    fillcolor = indicator["color"]

                if fillcolor is None:
                    scattergl = go.Scatter(
                        x=data['date_plot'],
                        y=data[name].values,
                        mode=mode,
                        name=name,
                        visible=visible
                    )
                else:
                    scattergl = go.Scatter(
                        x=data['date_plot'],
                        y=data[name].values,
                        mode=mode,
                        name=name,
                        visible=visible,
                        line={'color': fillcolor},
                    )

                overlay = True
                if "overlay" in indicator:
                    overlay = indicator["overlay"]

                if overlay:
                    fig.add_trace(scattergl, 1, 1)
                else:
                    fig.add_trace(scattergl, 2, 1)
            else:
                logger.info(
                    'Indicator "%s" ignored. Reason: This indicator is not found '
                    'in your strategy.',
                    indicator
                )

        fig.update_annotations({'font': {'size': 12}})
        fig.update_layout(template='plotly_white')

        return fig

    def _plot_trades(self, fig, trades) -> make_subplots:
        # Trades can be empty
        if trades is not None and len(trades) > 0:
            # color = trades.apply(lambda row: 'blue' if row["reduce_only"] else ('DarkGreen' if row['contracts'] > 0 else 'FireBrick'), axis=1)
            color = trades.apply(lambda row: 'orange' if row["order_profit"] == 0 else (
                'DarkGreen' if row['order_profit'] > 0 else 'FireBrick'), axis=1)

            shape = trades.apply(lambda row: ('triangle-up' if row['contracts'] > 0 else 'triangle-down'),
                                 axis=1)  # 'circle' if row["reduce_only"] else
            desc = trades.apply(lambda row: f"{row['format_string_html']}", axis=1)

            trace = go.Scatter(
                x=trades["close_time"],
                y=trades["price"],
                text=desc,
                mode='markers',
                name="Executed Orders",
                marker=dict(
                    symbol=shape,
                    size=10,
                    line=dict(width=0),
                    color=color
                )
            )

            fig.add_trace(trace, 1, 1)

            trace = go.Scatter(
                x=trades["creation_time"],
                y=trades["price"],
                mode='markers',
                name="Creation Orders",
                marker=dict(
                    symbol=shape,
                    size=8,
                    line=dict(width=0),
                    color="black"
                ),
                visible='legendonly'
            )
            fig.add_trace(trace, 1, 1)
        return fig

    def _plot_orders(self, fig, orders) -> make_subplots:
        # orders can be empty
        if orders is not None and len(orders) > 0:
            color = "black"
            desc = orders.apply(lambda row: f"Contracts: {row['contracts']}</br>Limit: {row['price']}", axis=1)
            shape = orders.apply(lambda row: ('circle' if row['contracts'] > 0 else 'circle'),
                                 axis=1)  # 'circle' if row["reduce_only"] else

            trace = go.Scatter(
                x=orders["current_time"],
                y=orders["price"],
                text=desc,
                mode='markers',
                name="Open Orders",
                marker=dict(
                    symbol=shape,
                    size=2,
                    line=dict(width=0),
                    color=color
                ),
                visible='legendonly'
            )
            fig.add_trace(trace, 1, 1)
        return fig

    def _plot_liquidations(self, fig, liquidations) -> make_subplots:
        # liquidations can be empty
        if liquidations is not None and len(liquidations) > 0:
            color = liquidations.apply(lambda row: 'red', axis=1)
            shape = liquidations.apply(lambda row: 'x-thin', axis=1)
            desc = liquidations.apply(lambda row: f"{row['format_string_html']}", axis=1)

            trace = go.Scatter(
                x=liquidations["creation_time"],
                y=liquidations["price"],
                text=desc,
                mode='markers',
                name="Liquidations",
                marker=dict(
                    symbol=shape,
                    size=15,
                    line=dict(width=1),
                    color=color
                ),
                visible='legendonly'
            )
            fig.add_trace(trace, 1, 1)
        return fig

    def _plot_k(self, fig) -> make_subplots:
        # liquidations can be empty
        if "best_k" in self.df_orig:
            desc = self.df_orig.apply(lambda row: f"{row['best_param']}", axis=1)

            trace = go.Scatter(
                x=self.df_orig["date_plot"],
                y=self.df_orig["best_k"],
                text=desc,
                name="BestParam",
                visible='legendonly'
            )
            fig.add_trace(trace, 3, 1)
        return fig








