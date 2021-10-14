import glob
import os
import sys
import pandas as pd
import tqdm
from config import Config

from indicator_util import IndicatorFiller
import os.path
from statements import *
from file_utils import alogger
import datetime
import traceback
from api.backtest_vectorized import VectorizedBacktest
from api.backtest_simple import SimpleBacktest
import dm_ticker
from api.backtest_simple import Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dm_utils import get_path

logger = logging.getLogger(__name__)

@alogger
def process_item(item):
    config = item.config
    file_utils.setup_logger_mp(config)
    try:
        logger.info("Starte worker_process_map mit PID: %d" % os.getpid())

        path_df = config.DIR_TEMP + '/' + item.id + ".csv"
        path_df_filled = config.DIR_TEMP + '/' + item.id + ".csv"
        path_db_out = f"{config.DIR_DB_OUT}/{item.config.GUID}.db"

        files = []
        files.append(path_db_out)
        files.append(path_df)
        files.append(path_df_filled)
        files.append(path_db_out)

        for file in files:
            if os.path.isfile(file):
                os.remove(file)

        # DB vorbereiten
        init_db_pairs_only(path_db_out)
        conn = db_utils.create_connection(path_db_out)
        cur = conn.cursor()
        end = datetime.datetime.utcfromtimestamp(item.zeitpunkt).strftime('%Y-%m-%d %H:%M:%S')
        start = datetime.datetime.utcfromtimestamp(item.zeitpunkt - ((item.candles - 2000) * item.timeframe * 60)).strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("insert into pairs (PAIR, START, ENDE, SPREAD, FEE) values (?, ?, ?, ?, ?)",
                    (item.pair, str(start), str(end), str(config.BACKTEST_SPREAD), str(config.BACKTEST_FEE)))
        conn.commit()
        pair_id = cur.lastrowid
        conn.close()

        # Daten holen
        postfix = "_" + str(os.getpid())
        path_json = get_path(item.pair, config.DIR_TEMP, postfix + ".json")
        if os.path.isfile(path_json):
            try:
                os.remove(path_json)
            except Exception as e:
                logger.error(e)
                return

        if config.TICKER_SOURCE == "binance":
            dm_ticker.update_ticker_binance(config.DIR_TEMP, item.timeframe, [item.pair], item.candles, zeitpunkt=item.zeitpunkt, postfix=postfix)
        elif config.TICKER_SOURCE == "binance_future_usdt":
            dm_ticker.update_ticker_binance_futures_usdt(config.DIR_TEMP, item.timeframe, [item.pair], item.candles, zeitpunkt=item.zeitpunkt, postfix=postfix)
        elif config.TICKER_SOURCE == "binance_future_coin":
            dm_ticker.update_ticker_binance_futures_coin(config.DIR_TEMP, item.timeframe, [item.pair], item.candles, zeitpunkt=item.zeitpunkt, postfix=postfix)
        else:
            logger.critical("unknown ticker source")

        # JSON zu DF
        df = file_utils.read_freqtrade_json(path_json)
        df.to_csv(path_df)
        logger.info("DF mit Indikatoren befüllen für: " + item.pair)

        f = IndicatorFiller()
        df = f.fill(df, minimal=False).iloc[2000:, :].reset_index()  # Die ersten 2000 weg weil Indikatoren NAN sein können
        df.to_csv(path_df_filled)

        if len(df.index) < 100:
            logger.critical("DF zu kurz. Abbrechen.")
            if os.path.isfile(path_db_out):
                os.remove(path_db_out)
            return False

        # STATEMENTS generieren
        columns = list(df.columns)
        columns = [x for x in columns if "chikou" not in x]
        columns = [x for x in columns if "date" not in x]
        columns = [x for x in columns if "Unnamed" not in x]
        columns = [x for x in columns if "level" not in x]
        columns = [x for x in columns if "index" not in x]
        columns = [x for x in columns if "_plot" not in x]
        info = parse_column_info(df, columns)

        statements = []
        random.seed(file_utils.get_seed())
        for i in range(config.SELECT_COUNT_PER_RUN):
            s = generate_statement_v2(info)
            statements.append(s)

        # Backtesten
        run_backtest(config, item, path_db_out, df, statements, pair_id)

        # Aufräumen
        files = glob.glob(config.DIR_TEMP + '/' + item.id + ".*")
        for f in files:
            os.remove(f)
    except Exception as e:
        logger.error(str(e))

@alogger
def run_backtest(config: Config, item, db_out: str, df: pd.DataFrame, statements: list, pair_id):
    id = item.id
    file_utils.setup_logger_mp(config)
    logger.info(f"{id} PID: {os.getpid()}")

    engine = create_engine('sqlite:///' + db_out, echo=False, connect_args={'timeout': 60})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()

    df["buy"] = 0
    df["sell"] = 0
    df["sbuy"] = 0
    df["ssell"] = 0
    bt = SimpleBacktest(df, ping_pong=config.SELECT_USE_PING_PONG, spread=config.BACKTEST_SPREAD, fee_pct=config.BACKTEST_FEE)
    vbt = VectorizedBacktest(df, spread=config.BACKTEST_SPREAD, fee_pct=config.BACKTEST_FEE, ping_pong=config.SELECT_USE_PING_PONG)

    db_buffer = []
    cnt = 0

    try:
        for i in tqdm.tqdm(range(len(statements))):
            s = statements[i]
            np_buy = apply_condition(df, s)
            np_sell = np.abs(np_buy - 1)

            # Schneller Backtest
            vbt.reset()
            vbt.backtest(np_buy, np_sell)

            if len(vbt.equity) == 0:
                logger.info("Länge von Equity ist null. Nächster Run...")
                return False

            if vbt.equity[-1] > 0:
                bt.reset()
                bt.backtest(np_buy, np_sell)
                if bt.stats is not None:
                    bt.stats.statement = s
                    bt.stats.pair_id = pair_id
                    db_buffer.append(bt.stats)
                    cnt += 1

            # Zwischenspeichern!
            if i % 10000 == 0:
                for k in range(len(db_buffer)):
                    session.add(db_buffer[k])
                session.commit()
                db_buffer = []

        logger.info(f"{id} buffer in db schreiben")
        for i in range(len(db_buffer)):
            session.add(db_buffer[i])
        session.commit()

    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
    finally:
        session.commit()
        session.close()
    return True