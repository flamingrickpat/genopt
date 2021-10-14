import logging
import time

import jstyleson

logger = logging.getLogger(__name__)

def get_path(pair, dir, ending):
    p = pair.replace("/", "-").replace("\\", "-")
    path = dir + "/" + p + ending
    return path

def get_timestamp_of_current_interval_start(interval):
    ts = int(time.time())
    tmp = int(ts / (interval * 60))
    return tmp * interval * 60

class Config():
    def __init__(self, config_json):
        # Diese werden intern gesetzt!
        self.GUID = ""
        self.LOG_PATH = "logs"
        self.COLUMN_TYPE_INFO_PATH = {}
        self.SHARED_STATUS_LENGTH = 0

        # Aus Datei...
        self.DIR_TEMP = "temp"
        self.DIR_DB_OUT = "out"
        self.COLUMN_TYPE_INFO = {}

        self.SELECT_USE_PING_PONG = True
        self.SELECT_COUNT_PER_RUN = 100000
        self.SELECT_CRITERIA_MIN = {}
        self.SELECT_CRITERIA_MAX = {}

        self.BACKTEST_SPREAD = 0
        self.BACKTEST_FEE = 0
        self.TICKER_SOURCE = "binance"

        # Aufwahlmöglichkeit TFs
        self.TIMEFRAMES = []
        # Aufwahlmöglichkeit Pairs
        self.PAIRS = []
        # Von - Bis als UTC Timestamp in Sekunden
        self.DATA_VON = 0
        self.DATA_BIS = 0
        self.CANDLES_MIN = 2000
        self.CANDLES_MAX = 4000
        # Anzahl Threads
        self.WORKER_COUNT = 64
        self.ENABLE_SUBPROCESS_LOGGING = False

        with open(config_json, "r") as f:
            s = f.read()
        c = jstyleson.loads(s)

        for key, value in c.items():
            if key != "TMP":
                try:
                    exec("tmp = self.%s" % key)
                except Exception as e:
                    logger.error("Ungültiger Parameter: %s" % key)
                    quit(1)

                try:
                    exec("self.%s = type(self.%s)(value)" % (key, key))
                    logger.info("Paramter: %s = %s" % (key, value))
                except Exception as e:
                    logger.error("Parameter %s hat ungültigen Typ: %s" % (key, value))
                    quit(1)

        logger.info("Config erfolgreich geladen.")



