import logging
import multiprocessing
import os.path
import random
import sys
import time
from multiprocessing import Process

import numpy as np

import db_utils
import file_utils
import indicator_util
from api import backtest_simple

logger = logging.getLogger(__name__)


def cached_get_column_type_info(path, cache_path, length=2000):
    # Neu befüllen order Cache laden
    if os.path.isfile(cache_path):
        df_type_info = file_utils.read(cache_path)
    else:
        logger.info("fill column_type_info_pair")
        df_type_info = file_utils.read(path)
        df_type_info = df_type_info.tail(length + 2000).copy().reset_index(drop=True)

        # So multiplizieren das Kurs richtung 10k geht, damit man Oscilaltoren und Overlap leichter unterscheiden kann anhand von Min und Max
        if df_type_info["close"].max() < 100:
            mul = 10000 / df_type_info["close"].max()
            df_type_info["open"] *= mul
            df_type_info["close"] *= mul
            df_type_info["high"] *= mul
            df_type_info["low"] *= mul
            df_type_info["volume"] *= mul

        # Befüllen
        f = indicator_util.IndicatorFiller()
        df_type_info = f.fill(df_type_info, minimal=False).tail(length).copy().reset_index(drop=True)
        logger.info("filled column_type_info_pair")

        # Cachen
        df_type_info.to_csv(cache_path)


    # Ungültige Columns droppen
    logger.info("get get_column_type_info")
    columns = list(df_type_info.columns)
    columns = [x for x in columns if "chikou" not in x]
    columns = [x for x in columns if "date" not in x]
    columns = [x for x in columns if "Unnamed" not in x]
    columns = [x for x in columns if "level" not in x]
    columns = [x for x in columns if "index" not in x]
    columns = [x for x in columns if "_plot" not in x]
    info = parse_column_info(df_type_info, columns)
    return info


def parse_column_info(df, columns):
    """
    Aus den Columns den Typ ermitteln. JSON-Dict zurückliefern.
    """
    crossover = {}
    oscillator = {}
    binary = {}

    for col in columns:
        d = {
                "min": df[col].min(),
                "max": df[col].max()
            }

        if df[col].nunique() <= 3:
            binary[col] = d
        else:
            try:
                # Wenn kleiner 0 oder zwischen 0 und 1 -> Oszillator
                if (df[col].min() < 0) or (df[col].min() >= 0 and df[col].max() <= 1):
                    oscillator[col] = d
                else:
                    crossover[col] = d
            except:
                pass

    data = {
        "crossover": crossover,
        "oscillator": oscillator,
        "binary": binary
    }

    return data

def apply_condition(df, s):
    """
    Statement wie String ausführen.
    """
    condition = '(' + s + ')'
    statement = 'df.loc[(' + condition + ' ),"buy"] = 1'
    df["buy"] = 0
    try:
        exec(statement)
    except Exception as e:
        logger.error(s + " EX: " + str(e))

    np_buy = np.copy(df["buy"].values)
    return np_buy

def generate_statement_v1(columns):
    """
    Kombiniert komplett zufällig die Columns.
    """
    cnt_comparisons = random.randrange(2, 10)

    s = ""
    for i in range(cnt_comparisons):
        cola = random.choice(columns)
        colb = random.choice(columns)
        operator = random.choice([">", "<"])

        shifta = ""
        if random.randrange(1, 100) > 50:
            shifta = ".shift(" + str(random.randrange(1, 200)) + ")"

        shiftb = ""
        if random.randrange(1, 100) > 50:
            shiftb = ".shift(" + str(random.randrange(1, 200)) + ")"

        row = f'(df["{cola}"]{shifta} {operator} df["{colb}"]{shiftb})'

        if i == 0:
            s = row
        else:
            s = s + " & " + row

    return s


def generate_statement_v2(type_info):
    """
    Kombiniert nur Columns vom gleichen Typ.
    """
    cnt_comparisons = random.randrange(2, 10)

    CROSSOVER = 1
    OSCILLATOR = 2
    BINARY = 3

    crossovers = list(type_info["crossover"].keys())
    oscillators = list(type_info["oscillator"].keys())
    binaries = list(type_info["binary"].keys())

    s = ""
    for i in range(cnt_comparisons):
        bshifta = False
        bshiftb = False
        bconstb = False

        t = random.choice([CROSSOVER, OSCILLATOR]) #, BINARY])
        if t == CROSSOVER:
            cola = random.choice(crossovers)
            colb = random.choice(crossovers)
            operator = random.choice([">", "<"])
            bshifta = True
            bshiftb = True
        elif t == OSCILLATOR:
            if random.randrange(0, 100) > 50:
                cola = random.choice(oscillators)
                colb = random.choice(oscillators)
                operator = random.choice([">", "<"])
                bshifta = True
                bshiftb = True
            else:
                cola = random.choice(oscillators)
                upper = type_info["oscillator"][cola]["max"] * 1.5
                lower = type_info["oscillator"][cola]["min"] * 0.75
                colb = random.uniform(lower, upper)
                operator = random.choice([">", "<"])
                bshifta = True
                bconstb = True

        elif t == BINARY:
            if random.randrange(0, 100) > 50:
                cola = random.choice(binaries)
                colb = random.choice(binaries)
                operator = "=="
                bshifta = True
                bshiftb = True
            else:
                cola = random.choice(binaries)
                colb = "0"
                operator = random.choice(["==", "!=", "<", ">"])
                bshifta = True
                bconstb = True

        shifta = ""
        if bshifta:
            if random.randrange(0, 100) > 50:
                shifta = ".shift(" + str(random.randrange(1, 200)) + ")"

        shiftb = ""
        if bshiftb:
            if random.randrange(0, 100) > 50:
                shiftb = ".shift(" + str(random.randrange(1, 200)) + ")"

        if bconstb:
            row = f'(df["{cola}"]{shifta} {operator} {colb})'
        else:
            row = f'(df["{cola}"]{shifta} {operator} df["{colb}"]{shiftb})'

        if i == 0:
            s = row
        else:
            s = s + " & " + row

    return s


def init_db_pairs_only(db_file):
    try:
        conn = db_utils.create_connection(db_file)
        cur = conn.cursor()
        cur.execute("CREATE TABLE `pairs` ( `ID` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, `PAIR` TEXT, `START` TEXT, `ENDE` TEXT, "
                    "`SPREAD` TEXT, `FEE` TEXT)")
        conn.commit()
    except:
        pass
