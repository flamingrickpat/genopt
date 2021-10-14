import datetime
import json
import logging
import os
import pickle
import sys
import time

import numpy
import numpy as np
import pandas as pd
from pandas import DataFrame, to_datetime


#  ██╗ ██╗     ██████╗  █████╗ ████████╗███████╗███╗   ██╗    ██╗      █████╗ ██████╗ ███████╗███╗   ██╗
# ████████╗    ██╔══██╗██╔══██╗╚══██╔══╝██╔════╝████╗  ██║    ██║     ██╔══██╗██╔══██╗██╔════╝████╗  ██║
# ╚██╔═██╔╝    ██║  ██║███████║   ██║   █████╗  ██╔██╗ ██║    ██║     ███████║██║  ██║█████╗  ██╔██╗ ██║
# ████████╗    ██║  ██║██╔══██║   ██║   ██╔══╝  ██║╚██╗██║    ██║     ██╔══██║██║  ██║██╔══╝  ██║╚██╗██║
# ╚██╔═██╔╝    ██████╔╝██║  ██║   ██║   ███████╗██║ ╚████║    ███████╗██║  ██║██████╔╝███████╗██║ ╚████║
#  ╚═╝ ╚═╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═══╝    ╚══════╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═══╝


def save_json(j, name):
    with open(name, 'w') as outfile:
        json.dump(j, outfile, cls=NumpyJsonEncoder, indent=4, sort_keys=True)

def load_json(name):
    with open(name) as json_file:
        data = json.load(json_file)
    return data


def save_obj(obj, name ):
    with open(name, 'wb') as f:
        pickle.dump(obj, f, 2) #pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name, 'rb') as f:
        return pickle.load(f)


def read_standard_csv(path, dropna=False, start=None, nrows=None):
    if start is None:
        skip = None
    else:
        skip = range(1, start)

    try:
        df = pd.read_csv(path,
                        sep=',',
                        parse_dates = ['date'],
                        infer_datetime_format = True,
                        skiprows=skip,
                        nrows=nrows,
                        index_col=0)
    except Exception as e:
        print(e)
        df = pd.read_csv(path,
                        sep='\t',
                        parse_dates = ['Time'],
                        infer_datetime_format = True,
                        skiprows=skip,
                        nrows=nrows,
                        index_col=0)
        df['Time'] = df['Time'].dt.tz_localize('UTC')
        df = df.rename(columns={'Time': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})


    if dropna:
        df = df.dropna()
    df = df.reset_index(drop=True).replace([np.inf, -np.inf], np.nan)
    return df


def read(path, dropna=False):
    if path.endswith("csv"):
        df = read_standard_csv(path, dropna=dropna)
    elif path.endswith("json"):
        df = read_freqtrade_json(path)
    elif path.endswith("pkl"):
        df = load_obj(path)
    else:
        raise Exception("unknown file type")

    df.drop_duplicates(subset=["date"], inplace=True, ignore_index=True)
    return df


def read_freqtrade_json(path) -> DataFrame:
    import rapidjson
    with open(path) as tickerdata:
        ohlcv = rapidjson.load(tickerdata, number_mode=rapidjson.NM_NATIVE)
    cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = DataFrame(ohlcv, columns=cols)

    df['date'] = to_datetime(df['date'], unit='ms', utc=True, infer_datetime_format=True)
    df = df.astype(dtype={'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
                          'volume': 'float'})
    return df


def write_freqtrade_json(df, path):
    result = df.to_json(orient="values")
    with open(path, "w") as f:
        f.write(result)


def get_default_df():
    todays_date = datetime.datetime.now().date()
    index = pd.date_range(todays_date - datetime.timedelta(10), periods=10, freq='D')

    columns = ['date', 'timestamp', 'open', 'high', 'low', 'close', 'volume']

    df = pd.DataFrame(index=index, columns=columns)
    df = df.fillna(0)
    return df.iloc[0:0].reset_index(drop=True)


#  ██╗ ██╗     ██████╗  █████╗ ████████╗███████╗████████╗██╗███╗   ███╗███████╗    ██╗  ██╗███████╗██╗     ██████╗ ███████╗██████╗
# ████████╗    ██╔══██╗██╔══██╗╚══██╔══╝██╔════╝╚══██╔══╝██║████╗ ████║██╔════╝    ██║  ██║██╔════╝██║     ██╔══██╗██╔════╝██╔══██╗
# ╚██╔═██╔╝    ██║  ██║███████║   ██║   █████╗     ██║   ██║██╔████╔██║█████╗      ███████║█████╗  ██║     ██████╔╝█████╗  ██████╔╝
# ████████╗    ██║  ██║██╔══██║   ██║   ██╔══╝     ██║   ██║██║╚██╔╝██║██╔══╝      ██╔══██║██╔══╝  ██║     ██╔═══╝ ██╔══╝  ██╔══██╗
# ╚██╔═██╔╝    ██████╔╝██║  ██║   ██║   ███████╗   ██║   ██║██║ ╚═╝ ██║███████╗    ██║  ██║███████╗███████╗██║     ███████╗██║  ██║
#  ╚═╝ ╚═╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝   ╚═╝   ╚═╝╚═╝     ╚═╝╚══════╝    ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝     ╚══════╝╚═╝  ╚═╝

def unix_to_pdts(unix):
    return pd.Timestamp(unix, unit='s', tz='UTC')

def pydt_to_pdts(pydt):
    return pd.Timestamp(pydt, unit='s', tz='UTC')

def na(val):
    if val is None:
        return 0
    if np.isnan(val) or np.isinf(val) or np.isneginf(val):
        return 0

    return val

class NumpyJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif isinstance(obj, numpy.bool_):
            return bool(obj)
        else:
            return super(NumpyJsonEncoder, self).default(obj)

def setup_logger_mp(config, level=logging.INFO):
    filename = os.path.join(config.LOG_PATH, config.GUID + "_" + str(os.getpid()) + ".log")
    setup_logger(filename, level=level, stdout=config.ENABLE_SUBPROCESS_LOGGING)


def setup_logger(filename=None, level=logging.INFO, stdout=True):
    root = logging.getLogger()
    root.handlers = []
    formatter = logging.Formatter('[%(asctime)s - [%(process)d] - %(name)s - %(funcName)s - %(levelname)s] %(message)s')

    if stdout:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        handler.setLevel(level)
        root.addHandler(handler)

    if filename is not None:
        fileHandler = logging.FileHandler(filename)
        fileHandler.setFormatter(formatter)
        fileHandler.setLevel(level)
        root.addHandler(fileHandler)

    root.setLevel(level)

def alogger(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        log = logging.getLogger(fn.__name__)
        log.info('ENTER %s' % fn.__name__)

        out = fn(*args, **kwargs)

        log.info('EXIT  %s' % fn.__name__)
        # Return the return value
        return out
    return wrapper


def get_seed():
    t = time.time_ns()
    pid = os.getpid()
    return t + pid
