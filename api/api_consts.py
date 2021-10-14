import datetime
import enum
import pprint

from file_utils import pydt_to_pdts

API_BYBIT = "bybit"
API_DERIBIT = "deribit"
API_BACKTEST = "backtest"


class OrderType(enum.Enum):
    MARKET = 1
    LIMIT = 2
    STOP_MARKET = 3
    LIQUIDATION = 4

    def __eq__(self, other):
        return self.value == other.value


class OrderSide(enum.Enum):
    BUY = 1
    SELL = 2

    def __eq__(self, other):
        return self.value == other.value


class OrderStatus(enum.Enum):
    NONE = 0
    ACTIVE = 1
    CLOSED = 2
    CANCELED = 3

    def __eq__(self, other):
        if self is None or other is None:
            return False
        return self.value == other.value


class PrintableContainer():
    def __init__(self):
        self.format_string = ""
        self.format_string_html = ""

    def __str__(self):
        """
        :return: Macht einen schön formatierten String von allen Variablen (ohne format_string damit die Daten nicht doppelt dastehen)
        """
        tmp = self.__dict__.copy()
        if "format_string" in tmp:
            del tmp["format_string"]
        if "format_string_html" in tmp:
            del tmp["format_string_html"]
        return pprint.pformat(tmp)

    def to_dict(self):
        """
        :return: Eine erweiterte Form von __dict__: Es wird vorher noch der Format-String erneuert und in HTML konvertiert.
        Damit man das rohe Dictonary + gesamte String Repräsentation für Pandas DF bekommt.
        """
        self.format_string = self.__str__()
        self.format_string_html = self.__str__().replace('\n', ' </br>')
        return self.__dict__

class Liquidation(PrintableContainer):
    def __init__(self):
        super().__init__()

        self.id = None
        self.price = None
        self.position_value = None
        self.positon_margin = None
        self.creation_time = None
        self.creation_i = None

class Order(PrintableContainer):
    def __init__(self):
        super().__init__()

        # Exchange
        self.id = None
        self.type = None
        self.price = None
        self.contracts = None
        self.status = None
        self.fee = 0
        self.reduce_only = False
        self.post_only = False
        self.oco_order = []

        # Zeitlich
        self.creation_time = None
        self.current_time = None
        self.open_time = None
        self.close_time = None
        self.cancel_time = None

        self.creation_i = None
        self.current_i = None
        self.open_i = None
        self.close_i = None
        self.cancel_i = None

        # Lifetime
        self.duration = 0
        self.max_lifetime = 0

        # Exchange Snapshot-Daten fürs Plotting
        # Werden nur bei Backtets verwendet
        self.deposit = None
        self.account_balance = None
        self.margin_balance = None
        self.available_balance = None
        self.upnl = None
        self.upnlp = None
        self.rpnl = None
        self.position_margin = None
        self.order_margin = None
        self.position = None
        self.average_entry_price = None
        self.order_profit = 0
        
        #self.tp = None
        #self.sl = None
        #self.tp_order = None
        #self.sl_order = None

    def cancel(self):
        raise Exception("not implemented")
        self.status = OrderStatus.CANCELED

    def set_price(self, price):
        raise Exception("not implemented")
        self.price = price
        
    #def set_tp(self, tp):
    #    if self.tp_order is not None:
    #        if self.tp_order.status == OrderStatus.ACTIVE:
    #            self.tp_order.price = tp
    #    if tp is None:
    #        if self.tp_order is not None and self.tp_order.status == OrderStatus.ACTIVE:
    #            self.tp_order.cancel()
    #        self.tp_order = None
    #        self.tp = None
    #
    #def set_sl(self, sl):
    #    if self.sl_order is not None:
    #        if self.sl_order.status == OrderStatus.ACTIVE:
    #            self.sl_order.price = sl
    #    if sl is None:
    #        if self.sl_order.status is not None and self.sl_order.status == OrderStatus.ACTIVE:
    #            self.sl_order.cancel()
    #        self.sl_order = None
    #        self.sl = None
#






    def set_params_bybit(self, order):
        """
                            Created - order accepted by the system but not yet put through matching engine
                            Rejected
                            New - order has placed successfully
                            PartiallyFilled
                            Filled
                            Cancelled
                            PendingCancel - the matching engine has received the cancellation but there is no guarantee that it will be successful
        """

        #{
        #    "order_id": "53c02936-2ba9-4fb8-afbf-9e6d31ee7314",
        #    "order_link_id": "",
        #    "symbol": "BTCUSD",
        #    "side": "Buy",
        #    "order_type": "Market",
        #    "price": "11071",
        #    "qty": 1,
        #    "time_in_force": "ImmediateOrCancel",
        #    "create_type": "CreateByClosing",
        #    "cancel_type": "",
        #    "order_status": "Filled",
        #    "leaves_qty": 0,
        #    "cum_exec_qty": 1,
        #    "cum_exec_value": "0.00009032",
        #    "cum_exec_fee": "0.00000007",
        #    "timestamp": "2020-09-19T15:26:54.290Z",
        #    "take_profit": "0",
        #    "stop_loss": "0",
        #    "trailing_stop": "0",
        #    "last_exec_price": "11071"
        #}

        self.id = order["order_id"]
        self.price = float(order["last_exec_price"])

        status = order["order_status"]
        if status == "Created":
            s = OrderStatus.ACTIVE
        elif status == "New":
            s = OrderStatus.ACTIVE
        elif status == "PartiallyFilled":
            s = OrderStatus.ACTIVE
        elif status == "Rejected":
            s = OrderStatus.CANCELED
        elif status == "Cancelled":
            s = OrderStatus.CANCELED
        elif status == "PendingCancel":
            s = OrderStatus.CANCELED
        elif status == "Filled":
            s = OrderStatus.CLOSED
        else:
            raise Exception("unknown order status")

        now = pydt_to_pdts(datetime.datetime.utcnow())
        if self.status is None:
            self.creation_time = now
            self.status = OrderStatus.NONE # Damit Vergleich möglich ist!

        if s == OrderStatus.ACTIVE and self.status != OrderStatus.ACTIVE:
            self.open_time = now
        if s == OrderStatus.CLOSED and self.status != OrderStatus.CLOSED:
            self.close_time = now
        if s == OrderStatus.CANCELED and self.status != OrderStatus.CANCELED:
            self.cancel_time = now

        self.status = s

        if order["side"] == "Buy":
            self.contracts = abs(int(order["qty"]))
        if order["side"] == "Sell":
            self.contracts = -1 * abs(int(order["qty"]))

        if order["order_type"] == "Market":
            self.type = OrderType.MARKET
        elif order["order_type"] == "Limit":
            self.type = OrderType.LIMIT
        else:
            raise Exception("unknown order type")

        if "reduce_only" in order:
            self.reduce_only = order["reduce_only"]



