import logging
import time

import jstyleson

logger = logging.getLogger(__name__)

GET_BEST_STRAT = 1
GET_SIGNAL = 2

TRADE_MODE_DEFAULT = 1
TRADE_MODE_MULTISTRAT = 2

class WorkItem():
    def __init__(self):
        self.pair = ""
        self.operation = 0
        self.candle_count = 0

def get_path(pair, dir, ending):
    p = pair.replace("/", "-").replace("\\", "-")
    path = dir + "/" + p + ending
    return path

def get_timestamp_of_current_interval_start(interval):
    ts = int(time.time())
    tmp = int(ts / (interval * 60))
    return tmp * interval * 60

def get_canldes_from_timeframe_and_days(tf, days):
    cpd = 24 * (60 / tf)
    return int(cpd * days)

def get_canldes_from_timeframe_and_hours(tf, hours):
    return int(hours * (60 / tf))