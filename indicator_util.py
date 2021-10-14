import logging

import pandas as pd
import talib
from finta import TA as fin

logger = logging.getLogger(__name__)

def add_indicators_and_rename(df, previous_cols, prefix, name, series):
    nn = prefix + name + '_'

    if series is None:
        for col in df.columns:
            if col not in previous_cols:
                logger.debug("Added: " + nn + col)
                df.rename(columns={col: nn + col}, inplace=True)

    if isinstance(series, pd.DataFrame):
        for col in series.columns:
            if col not in previous_cols:
                logger.debug("Added: " + nn + col)
                df.rename(columns={col: nn + col}, inplace=True)

    if isinstance(series, pd.Series):
        logger.debug("Added: " + prefix + name)
        df[prefix + name] = series

    return df


class IndicatorFiller():

    def fill(self, input_df, minimal=False, indicators=None):
        import pandas as pd
        import warnings

        warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

        self.cols = input_df.columns
        MINIMAL_LENGTH = 2000

        def add(name, series, pre):
            add_indicators_and_rename(df, previous_cols=self.cols, prefix=pre, name=name, series=series)
            self.cols = df.columns

        def set_cols():
            self.cols = df.columns


        def check_indicator(name):
            if indicators is None:
                return True
            else:
                for indicator in indicators:
                    if name in indicator or name == indicator:
                        return True
                return False


        def check_indicator_add(name, prefix):
            nn = prefix + name
            if indicators is None:
                return True
            else:
                for indicator in indicators:
                    if nn in indicator or nn == indicator:
                        return True
                return False

        if minimal:
            df = input_df.tail(MINIMAL_LENGTH).copy().reset_index(drop=True)
        else:
            df = input_df.copy()

        close = df["close"]
        high = df["high"]
        low = df["low"]
        open = df["open"]
        volume = df["volume"]
        prefix = "fixed_"

        #    _____     _______ ____  _        _    ____
        #   / _ \ \   / / ____|  _ \| |      / \  |  _ \
        #  | | | \ \ / /|  _| | |_) | |     / _ \ | |_) |
        #  | |_| |\ V / | |___|  _ <| |___ / ___ \|  __/
        #   \___/  \_/  |_____|_| \_\_____/_/   \_\_|
        if check_indicator(prefix + "HT_TRENDLINE"):
            real = talib.HT_TRENDLINE(close)
            df[prefix + "HT_TRENDLINE"] = real

        #if check_indicator(prefix + "SAR"):
        #       real = talib.SAR(high, low, acceleration=0, maximum=0)
        #        df[prefix + "SAR"] = real

        #if check_indicator(prefix + "SAREXT"):
        #       real = talib.SAREXT(high, low, startvalue=0, offsetonreverse=0, accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0, accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)
        #        df[prefix + "SAREXT"] = real

        #    ___  ____   ____ ___ _     _        _  _____ ___  ____
        #   / _ \/ ___| / ___|_ _| |   | |      / \|_   _/ _ \|  _ \
        #  | | | \___ \| |    | || |   | |     / _ \ | || | | | |_) |
        #  | |_| |___) | |___ | || |___| |___ / ___ \| || |_| |  _ <
        #   \___/|____/ \____|___|_____|_____/_/   \_\_| \___/|_| \_\
        if check_indicator(prefix + "BOP"):
            real = talib.BOP(open, high, low, close)
            df[prefix + "BOP"] = real

        #  __     _____  _    _   _ __  __ _____
        #  \ \   / / _ \| |  | | | |  \/  | ____|
        #   \ \ / / | | | |  | | | | |\/| |  _|
        #    \ V /| |_| | |__| |_| | |  | | |___
        #     \_/  \___/|_____\___/|_|  |_|_____|
        #if check_indicator(prefix + "AD"):
        #       real = talib.AD(high, low, close, volume)
        #        df[prefix + "AD"] = real
        #if check_indicator(prefix + "OBV"):
        #       real = talib.OBV(close, volume)
        #        df[prefix + "OBV"] = real

        #  __     _____  _        _  _____ ___ _     ___ _______   __
        #  \ \   / / _ \| |      / \|_   _|_ _| |   |_ _|_   _\ \ / /
        #   \ \ / / | | | |     / _ \ | |  | || |    | |  | |  \ V /
        #    \ V /| |_| | |___ / ___ \| |  | || |___ | |  | |   | |
        #     \_/  \___/|_____/_/   \_\_| |___|_____|___| |_|   |_|
        if check_indicator(prefix + "TRANGE"):
            real = talib.TRANGE(high, low, close)
            df[prefix + "TRANGE"] = real

        #   ____  ____  ___ ____ _____
        #  |  _ \|  _ \|_ _/ ___| ____|
        #  | |_) | |_) || | |   |  _|
        #  |  __/|  _ < | | |___| |___
        #  |_|   |_| \_\___\____|_____|
        if check_indicator(prefix + "AVGPRICE"):
            real = talib.AVGPRICE(open, high, low, close)
            df[prefix + "AVGPRICE"] = real

        if check_indicator(prefix + "MEDPRICE"):
            real = talib.MEDPRICE(high, low)
            df[prefix + "MEDPRICE"] = real

        if check_indicator(prefix + "TYPPRICE"):
            real = talib.TYPPRICE(high, low, close)
            df[prefix + "TYPPRICE"] = real

        if check_indicator(prefix + "WCLPRICE"):
            real = talib.WCLPRICE(high, low, close)
            df[prefix + "WCLPRICE"] = real

        #    ______   ______ _     _____
        #   / ___\ \ / / ___| |   | ____|
        #  | |    \ V / |   | |   |  _|
        #  | |___  | || |___| |___| |___
        #   \____| |_| \____|_____|_____|
        if check_indicator(prefix + "HT_DCPERIOD"):
            real = talib.HT_DCPERIOD(close)
            df[prefix + "HT_DCPERIOD"] = real

        if check_indicator(prefix + "HT_DCPHASE"):
            real = talib.HT_DCPHASE(close)
            df[prefix + "HT_DCPHASE"] = real

        inphase, quadrature = talib.HT_PHASOR(close)
        df[prefix + "inphase"] = inphase
        df[prefix + "quadrature"] = quadrature

        sine, leadsine = talib.HT_SINE(close)
        df[prefix + "sine"] = sine
        df[prefix + "leadsine"] = leadsine

        if check_indicator(prefix + "HT_TRENDMODE"):
            integer = talib.HT_TRENDMODE(close)
            df[prefix + "HT_TRENDMODE"] = integer

        #   ____   _  _____ _____ _____ ____  _   _
        #  |  _ \ / \|_   _|_   _| ____|  _ \| \ | |
        #  | |_) / _ \ | |   | | |  _| | |_) |  \| |
        #  |  __/ ___ \| |   | | | |___|  _ <| |\  |
        #  |_| /_/   \_\_|   |_| |_____|_| \_\_| \_|
        if check_indicator(prefix + "CDL2CROWS"):
            integer = talib.CDL2CROWS(open, high, low, close)
            df[prefix + "CDL2CROWS"] = integer
        if check_indicator(prefix + "CDL3BLACKCROWS"):
            integer = talib.CDL3BLACKCROWS(open, high, low, close)
            df[prefix + "CDL3BLACKCROWS"] = integer
        if check_indicator(prefix + "CDL3INSIDE"):
            integer = talib.CDL3INSIDE(open, high, low, close)
            df[prefix + "CDL3INSIDE"] = integer
        if check_indicator(prefix + "CDL3LINESTRIKE"):
            integer = talib.CDL3LINESTRIKE(open, high, low, close)
            df[prefix + "CDL3LINESTRIKE"] = integer
        if check_indicator(prefix + "CDL3OUTSIDE"):
            integer = talib.CDL3OUTSIDE(open, high, low, close)
            df[prefix + "CDL3OUTSIDE"] = integer
        if check_indicator(prefix + "CDL3STARSINSOUTH"):
            integer = talib.CDL3STARSINSOUTH(open, high, low, close)
            df[prefix + "CDL3STARSINSOUTH"] = integer
        if check_indicator(prefix + "CDL3WHITESOLDIERS"):
            integer = talib.CDL3WHITESOLDIERS(open, high, low, close)
            df[prefix + "CDL3WHITESOLDIERS"] = integer
        if check_indicator(prefix + "CDLABANDONEDBABY"):
            integer = talib.CDLABANDONEDBABY(open, high, low, close, penetration=0)
            df[prefix + "CDLABANDONEDBABY"] = integer
        if check_indicator(prefix + "CDLADVANCEBLOCK"):
            integer = talib.CDLADVANCEBLOCK(open, high, low, close)
            df[prefix + "CDLADVANCEBLOCK"] = integer
        if check_indicator(prefix + "CDLBELTHOLD"):
            integer = talib.CDLBELTHOLD(open, high, low, close)
            df[prefix + "CDLBELTHOLD"] = integer
        if check_indicator(prefix + "CDLBREAKAWAY"):
            integer = talib.CDLBREAKAWAY(open, high, low, close)
            df[prefix + "CDLBREAKAWAY"] = integer
        if check_indicator(prefix + "CDLCLOSINGMARUBOZU"):
            integer = talib.CDLCLOSINGMARUBOZU(open, high, low, close)
            df[prefix + "CDLCLOSINGMARUBOZU"] = integer
        if check_indicator(prefix + "CDLCONCEALBABYSWALL"):
            integer = talib.CDLCONCEALBABYSWALL(open, high, low, close)
            df[prefix + "CDLCONCEALBABYSWALL"] = integer
        if check_indicator(prefix + "CDLCOUNTERATTACK"):
            integer = talib.CDLCOUNTERATTACK(open, high, low, close)
            df[prefix + "CDLCOUNTERATTACK"] = integer
        if check_indicator(prefix + "CDLDARKCLOUDCOVER"):
            integer = talib.CDLDARKCLOUDCOVER(open, high, low, close, penetration=0)
            df[prefix + "CDLDARKCLOUDCOVER"] = integer
        if check_indicator(prefix + "CDLDOJI"):
            integer = talib.CDLDOJI(open, high, low, close)
            df[prefix + "CDLDOJI"] = integer
        if check_indicator(prefix + "CDLDOJISTAR"):
            integer = talib.CDLDOJISTAR(open, high, low, close)
            df[prefix + "CDLDOJISTAR"] = integer
        if check_indicator(prefix + "CDLDRAGONFLYDOJI"):
            integer = talib.CDLDRAGONFLYDOJI(open, high, low, close)
            df[prefix + "CDLDRAGONFLYDOJI"] = integer
        if check_indicator(prefix + "CDLENGULFING"):
            integer = talib.CDLENGULFING(open, high, low, close)
            df[prefix + "CDLENGULFING"] = integer
        if check_indicator(prefix + "CDLEVENINGDOJISTAR"):
            integer = talib.CDLEVENINGDOJISTAR(open, high, low, close, penetration=0)
            df[prefix + "CDLEVENINGDOJISTAR"] = integer
        if check_indicator(prefix + "CDLEVENINGSTAR"):
            integer = talib.CDLEVENINGSTAR(open, high, low, close, penetration=0)
            df[prefix + "CDLEVENINGSTAR"] = integer
        if check_indicator(prefix + "CDLGAPSIDESIDEWHITE"):
            integer = talib.CDLGAPSIDESIDEWHITE(open, high, low, close)
            df[prefix + "CDLGAPSIDESIDEWHITE"] = integer
        if check_indicator(prefix + "CDLGRAVESTONEDOJI"):
            integer = talib.CDLGRAVESTONEDOJI(open, high, low, close)
            df[prefix + "CDLGRAVESTONEDOJI"] = integer
        if check_indicator(prefix + "CDLHAMMER"):
            integer = talib.CDLHAMMER(open, high, low, close)
            df[prefix + "CDLHAMMER"] = integer
        if check_indicator(prefix + "CDLHANGINGMAN"):
            integer = talib.CDLHANGINGMAN(open, high, low, close)
            df[prefix + "CDLHANGINGMAN"] = integer
        if check_indicator(prefix + "CDLHARAMI"):
            integer = talib.CDLHARAMI(open, high, low, close)
            df[prefix + "CDLHARAMI"] = integer
        if check_indicator(prefix + "CDLHARAMICROSS"):
            integer = talib.CDLHARAMICROSS(open, high, low, close)
            df[prefix + "CDLHARAMICROSS"] = integer
        if check_indicator(prefix + "CDLHIGHWAVE"):
            integer = talib.CDLHIGHWAVE(open, high, low, close)
            df[prefix + "CDLHIGHWAVE"] = integer
        if check_indicator(prefix + "CDLHIKKAKE"):
            integer = talib.CDLHIKKAKE(open, high, low, close)
            df[prefix + "CDLHIKKAKE"] = integer
        if check_indicator(prefix + "CDLHIKKAKEMOD"):
            integer = talib.CDLHIKKAKEMOD(open, high, low, close)
            df[prefix + "CDLHIKKAKEMOD"] = integer
        if check_indicator(prefix + "CDLHOMINGPIGEON"):
            integer = talib.CDLHOMINGPIGEON(open, high, low, close)
            df[prefix + "CDLHOMINGPIGEON"] = integer
        if check_indicator(prefix + "CDLIDENTICAL3CROWS"):
            integer = talib.CDLIDENTICAL3CROWS(open, high, low, close)
            df[prefix + "CDLIDENTICAL3CROWS"] = integer
        if check_indicator(prefix + "CDLINNECK"):
            integer = talib.CDLINNECK(open, high, low, close)
            df[prefix + "CDLINNECK"] = integer
        if check_indicator(prefix + "CDLINVERTEDHAMMER"):
            integer = talib.CDLINVERTEDHAMMER(open, high, low, close)
            df[prefix + "CDLINVERTEDHAMMER"] = integer
        if check_indicator(prefix + "CDLKICKING"):
            integer = talib.CDLKICKING(open, high, low, close)
            df[prefix + "CDLKICKING"] = integer
        if check_indicator(prefix + "CDLKICKINGBYLENGTH"):
            integer = talib.CDLKICKINGBYLENGTH(open, high, low, close)
            df[prefix + "CDLKICKINGBYLENGTH"] = integer
        if check_indicator(prefix + "CDLLADDERBOTTOM"):
            integer = talib.CDLLADDERBOTTOM(open, high, low, close)
            df[prefix + "CDLLADDERBOTTOM"] = integer
        if check_indicator(prefix + "CDLLONGLEGGEDDOJI"):
            integer = talib.CDLLONGLEGGEDDOJI(open, high, low, close)
            df[prefix + "CDLLONGLEGGEDDOJI"] = integer
        if check_indicator(prefix + "CDLLONGLINE"):
            integer = talib.CDLLONGLINE(open, high, low, close)
            df[prefix + "CDLLONGLINE"] = integer
        if check_indicator(prefix + "CDLMARUBOZU"):
            integer = talib.CDLMARUBOZU(open, high, low, close)
            df[prefix + "CDLMARUBOZU"] = integer
        if check_indicator(prefix + "CDLMATCHINGLOW"):
            integer = talib.CDLMATCHINGLOW(open, high, low, close)
            df[prefix + "CDLMATCHINGLOW"] = integer
        if check_indicator(prefix + "CDLMATHOLD"):
            integer = talib.CDLMATHOLD(open, high, low, close, penetration=0)
            df[prefix + "CDLMATHOLD"] = integer
        if check_indicator(prefix + "CDLMORNINGDOJISTAR"):
            integer = talib.CDLMORNINGDOJISTAR(open, high, low, close, penetration=0)
            df[prefix + "CDLMORNINGDOJISTAR"] = integer
        if check_indicator(prefix + "CDLMORNINGSTAR"):
            integer = talib.CDLMORNINGSTAR(open, high, low, close, penetration=0)
            df[prefix + "CDLMORNINGSTAR"] = integer
        if check_indicator(prefix + "CDLONNECK"):
            integer = talib.CDLONNECK(open, high, low, close)
            df[prefix + "CDLONNECK"] = integer
        if check_indicator(prefix + "CDLPIERCING"):
            integer = talib.CDLPIERCING(open, high, low, close)
            df[prefix + "CDLPIERCING"] = integer
        if check_indicator(prefix + "CDLRICKSHAWMAN"):
            integer = talib.CDLRICKSHAWMAN(open, high, low, close)
            df[prefix + "CDLRICKSHAWMAN"] = integer
        if check_indicator(prefix + "CDLRISEFALL3METHODS"):
            integer = talib.CDLRISEFALL3METHODS(open, high, low, close)
            df[prefix + "CDLRISEFALL3METHODS"] = integer
        if check_indicator(prefix + "CDLSEPARATINGLINES"):
            integer = talib.CDLSEPARATINGLINES(open, high, low, close)
            df[prefix + "CDLSEPARATINGLINES"] = integer
        if check_indicator(prefix + "CDLSHOOTINGSTAR"):
            integer = talib.CDLSHOOTINGSTAR(open, high, low, close)
            df[prefix + "CDLSHOOTINGSTAR"] = integer
        if check_indicator(prefix + "CDLSHORTLINE"):
            integer = talib.CDLSHORTLINE(open, high, low, close)
            df[prefix + "CDLSHORTLINE"] = integer
        if check_indicator(prefix + "CDLSPINNINGTOP"):
            integer = talib.CDLSPINNINGTOP(open, high, low, close)
            df[prefix + "CDLSPINNINGTOP"] = integer
        if check_indicator(prefix + "CDLSTALLEDPATTERN"):
            integer = talib.CDLSTALLEDPATTERN(open, high, low, close)
            df[prefix + "CDLSTALLEDPATTERN"] = integer
        if check_indicator(prefix + "CDLSTICKSANDWICH"):
            integer = talib.CDLSTICKSANDWICH(open, high, low, close)
            df[prefix + "CDLSTICKSANDWICH"] = integer
        if check_indicator(prefix + "CDLTAKURI"):
            integer = talib.CDLTAKURI(open, high, low, close)
            df[prefix + "CDLTAKURI"] = integer
        if check_indicator(prefix + "CDLTASUKIGAP"):
            integer = talib.CDLTASUKIGAP(open, high, low, close)
            df[prefix + "CDLTASUKIGAP"] = integer
        if check_indicator(prefix + "CDLTHRUSTING"):
            integer = talib.CDLTHRUSTING(open, high, low, close)
            df[prefix + "CDLTHRUSTING"] = integer
        if check_indicator(prefix + "CDLTRISTAR"):
            integer = talib.CDLTRISTAR(open, high, low, close)
            df[prefix + "CDLTRISTAR"] = integer
        if check_indicator(prefix + "CDLUNIQUE3RIVER"):
            integer = talib.CDLUNIQUE3RIVER(open, high, low, close)
            df[prefix + "CDLUNIQUE3RIVER"] = integer
        if check_indicator(prefix + "CDLUPSIDEGAP2CROWS"):
            integer = talib.CDLUPSIDEGAP2CROWS(open, high, low, close)
            df[prefix + "CDLUPSIDEGAP2CROWS"] = integer
        if check_indicator(prefix + "CDLXSIDEGAP3METHODS"):
            integer = talib.CDLXSIDEGAP3METHODS(open, high, low, close)
            df[prefix + "CDLXSIDEGAP3METHODS"] = integer

        #   _____ ___ _   _ _____  _
        #  |  ___|_ _| \ | |_   _|/ \
        #  | |_   | ||  \| | | | / _ \
        #  |  _|  | || |\  | | |/ ___ \
        #  |_|   |___|_| \_| |_/_/   \_\


        #real = fin.DYMI(df)
        #add("DYMI", real, prefix)
        set_cols()
        if check_indicator_add("PSAR", prefix):
            real = fin.PSAR(df)
            add("PSAR", real, prefix)

        if check_indicator_add("KST", prefix):  
            tmp = fin.KST(df)
            add("KST", tmp, prefix)

        if check_indicator_add("COPP", prefix):  
            real = fin.COPP(df)
            add("COPP", real, prefix)

        if check_indicator_add("PIVOT", prefix):  
            tmp = fin.PIVOT(df)
            add("PIVOT", tmp, prefix)

        if check_indicator_add("PIVOT_FIB", prefix):  
            tmp = fin.PIVOT_FIB(df)
            add("PIVOT_FIB", tmp, prefix)

        if check_indicator_add("UO", prefix):  
            real = fin.UO(df)
            add("UO", real, prefix)

        if check_indicator_add("EBBP", prefix):  
            tmp = fin.EBBP(df)
            add("EBBP", tmp, prefix)

        #    ____ _   _ ____ _____ ___  __  __
        #   / ___| | | / ___|_   _/ _ \|  \/  |
        #  | |   | | | \___ \ | || | | | |\/| |
        #  | |___| |_| |___) || || |_| | |  | |
        #   \____|\___/|____/ |_| \___/|_|  |_|
        #if check_indicator_add("wavepm_bands", prefix):
        #    tmp = cust.calculate_wavepm_bands(df, lookback=100, wavepm_column="close", periods=None)
        #    df.drop(list(df.filter(regex='wavePM')), axis=1, inplace=True)
        #    tmp.drop(list(df.filter(regex='wavePM')), axis=1, inplace=True)
        #    add("wavepm_bands", tmp, prefix)

        #if check_indicator_add("log_returns", prefix):
        #    tmp = cust.log_returns(close)
        #    add("log_returns", tmp, prefix)
        #tmp = cust.sessions(df)
        # add("sessions", tmp, prefix)
        #if check_indicator_add("pivots_daily_weekly", prefix):
        #    tmp = cust.pivots_daily_weekly(df)
        #    add("pivots_daily_weekly", tmp, prefix)

        #if check_indicator_add("signal_noise_ratio", prefix):
        #    tmp = cust.signal_noise_ratio(df)
        #    add("signal_noise_ratio", tmp, prefix)

        #if check_indicator_add("range_filter_dw", prefix):
        #    tmp = cust.range_filter_dw(df)
        #    add("range_filter_dw", tmp, prefix)

        periods = [7, 9, 14, 20, 30, 40, 50, 75, 100, 125, 150, 175, 200, 300, 400]
        periods.reverse()
        for period in periods:
            # Beim Minimal-Modus wird immer weniger genommen damits schneller geht. Man braucht eh nur -1.
            if minimal:
                df = df.tail(max(300, period * 4)).copy().reset_index(drop=True)

            logger.debug(f"Period: {period}")

            precision_check = period <= 300

            prefix = str(period) + "_"
            period02 = max(2, int(period * 0.2))
            period03 = max(2, int(period * 0.33))
            period05 = max(2, int(period * 0.5))
            period07 = max(2, int(period * 0.75))

            #    _____     _______ ____  _        _    ____
            #   / _ \ \   / / ____|  _ \| |      / \  |  _ \
            #  | | | \ \ / /|  _| | |_) | |     / _ \ | |_) |
            #  | |_| |\ V / | |___|  _ <| |___ / ___ \|  __/
            #   \___/  \_/  |_____|_| \_\_____/_/   \_\_|
            if check_indicator(prefix + "upperband125") or check_indicator(prefix + "middleband125") or check_indicator(prefix + "lowerband125"):
                upperband, middleband, lowerband = talib.BBANDS(close, timeperiod=period, nbdevup=1.25, nbdevdn=1.25, matype=0)
                df[prefix + "upperband125"] = upperband
                df[prefix + "middleband125"] = middleband
                df[prefix + "lowerband125"] = lowerband

            if check_indicator(prefix + "upperband200") or check_indicator(prefix + "middleband200") or check_indicator(prefix + "lowerband200"):
                upperband, middleband, lowerband = talib.BBANDS(close, timeperiod=period, nbdevup=2, nbdevdn=2, matype=0)
                df[prefix + "upperband200"] = upperband
                df[prefix + "middleband200"] = middleband
                df[prefix + "lowerband200"] = lowerband

            if check_indicator(prefix + "DEMA"):
                real = talib.DEMA(close, timeperiod=period)
                df[prefix + "DEMA"] = real

            if check_indicator(prefix + "EMA"):
                real = talib.EMA(close, timeperiod=period)
                df[prefix + "EMA"] = real

            if check_indicator(prefix + "KAMA"):
                real = talib.KAMA(close, timeperiod=period)
                df[prefix + "KAMA"] = real

            if check_indicator(prefix + "MIDPOINT"):
                real = talib.MIDPOINT(close, timeperiod=period)
                df[prefix + "MIDPOINT"] = real

            if check_indicator(prefix + "MIDPRICE"):
                real = talib.MIDPRICE(high, low, timeperiod=period)
                df[prefix + "MIDPRICE"] = real

            if check_indicator(prefix + "SMA"):
                real = talib.SMA(close, timeperiod=period)
                df[prefix + "SMA"] = real

            if check_indicator(prefix + "T3"):
                real = talib.T3(close, timeperiod=period, vfactor=0)
                df[prefix + "T3"] = real

            if check_indicator(prefix + "TEMA"):
                real = talib.TEMA(close, timeperiod=period)
                df[prefix + "TEMA"] = real

            if check_indicator(prefix + "TRIMA"):
                real = talib.TRIMA(close, timeperiod=period)
                df[prefix + "TRIMA"] = real

            if check_indicator(prefix + "WMA"):
                real = talib.WMA(close, timeperiod=period)
                df[prefix + "WMA"] = real

            #   __  __  ___  __  __ _____ _   _ _____ _   _ __  __
            #  |  \/  |/ _ \|  \/  | ____| \ | |_   _| | | |  \/  |
            #  | |\/| | | | | |\/| |  _| |  \| | | | | | | | |\/| |
            #  | |  | | |_| | |  | | |___| |\  | | | | |_| | |  | |
            #  |_|  |_|\___/|_|  |_|_____|_| \_| |_|  \___/|_|  |_|
            if precision_check:
                if check_indicator(prefix + "ADX"):
                    real = talib.ADX(high, low, close, timeperiod=period)
                    df[prefix + "ADX"] = real

                if check_indicator(prefix + "ADXR"):
                    real = talib.ADXR(high, low, close, timeperiod=period)
                    df[prefix + "ADXR"] = real

            if check_indicator(prefix + "APO"):
                real = talib.APO(close, fastperiod=period05, slowperiod=period, matype=0)
                df[prefix + "APO"] = real

            if check_indicator(prefix + "AROONOSC"):
                real = talib.AROONOSC(high, low, timeperiod=period)
                df[prefix + "AROONOSC"] = real

            if check_indicator(prefix + "CCI"):
                real = talib.CCI(high, low, close, timeperiod=period)
                df[prefix + "CCI"] = real

            if precision_check:
                if check_indicator(prefix + "CMO"):
                    real = talib.CMO(close, timeperiod=period)
                    df[prefix + "CMO"] = real

            if check_indicator(prefix + "DX"):
                real = talib.DX(high, low, close, timeperiod=period)
                df[prefix + "DX"] = real

            if check_indicator(prefix + "MFI"):
                real = talib.MFI(high, low, close, volume, timeperiod=period)
                df[prefix + "MFI"] = real

            if check_indicator(prefix + "MINUS_DI"):
                real = talib.MINUS_DI(high, low, close, timeperiod=period)
                df[prefix + "MINUS_DI"] = real

            if check_indicator(prefix + "MINUS_DM"):
                real = talib.MINUS_DM(high, low, timeperiod=period)
                df[prefix + "MINUS_DM"] = real

            if check_indicator(prefix + "MOM"):
                real = talib.MOM(close, timeperiod=period)
                df[prefix + "MOM"] = real

            if check_indicator(prefix + "PLUS_DI"):
                real = talib.PLUS_DI(high, low, close, timeperiod=period)
                df[prefix + "PLUS_DI"] = real

            if check_indicator(prefix + "PLUS_DM"):
                real = talib.PLUS_DM(high, low, timeperiod=period)
                df[prefix + "PLUS_DM"] = real

            if check_indicator(prefix + "PPO"):
                real = talib.PPO(close, fastperiod=period05, slowperiod=period, matype=0)
                df[prefix + "PPO"] = real

            if check_indicator(prefix + "ROC"):
                real = talib.ROC(close, timeperiod=period)
                df[prefix + "ROC"] = real

            if check_indicator(prefix + "ROCP"):
                real = talib.ROCP(close, timeperiod=period)
                df[prefix + "ROCP"] = real

            if check_indicator(prefix + "ROCR"):
                real = talib.ROCR(close, timeperiod=period)
                df[prefix + "ROCR"] = real

            if check_indicator(prefix + "ROCR100"):
                real = talib.ROCR100(close, timeperiod=period)
                df[prefix + "ROCR100"] = real

            if check_indicator(prefix + "RSI"):
                real = talib.RSI(close, timeperiod=period)
                df[prefix + "RSI"] = real

            if check_indicator(prefix + "TRIX"):
                real = talib.TRIX(close, timeperiod=period)
                df[prefix + "TRIX"] = real

            if check_indicator(prefix + "ULTOSC"):
                real = talib.ULTOSC(high, low, close, timeperiod1=period05, timeperiod2=period07, timeperiod3=period)
                df[prefix + "ULTOSC"] = real

            if check_indicator(prefix + "WILLR"):
                real = talib.WILLR(high, low, close, timeperiod=period)
                df[prefix + "WILLR"] = real

            if check_indicator(prefix + "aroon"):
                aroondown, aroonup = talib.AROON(high, low, timeperiod=period)
                df[prefix + "aroondown"] = aroondown
                df[prefix + "aroonup"] = aroonup

            if check_indicator(prefix + "macd"):
                macd, macdsignal, macdhist = talib.MACD(close, fastperiod=period05, slowperiod=period, signalperiod=period07)
                df[prefix + "macd"] = macd
                df[prefix + "macdsignal"] = macdsignal
                df[prefix + "macdhist"] = macdhist

            # macd, macdsignal, macdhist = talib.MACDEXT(close, fastperiod=period05, fastmatype=0, slowperiod=period, slowmatype=0, signalperiod=period07, signalmatype=0)
            # df[prefix + "macd2"] = macd
            # df[prefix + "macdsignal2"] = macdsignal
            # df[prefix + "macdhist2"] = macdhist

            # macd, macdsignal, macdhist = talib.MACDFIX(close, signalperiod=period)
            # df[prefix + "macd3"] = macd
            # df[prefix + "macdsignal3"] = macdsignal
            # df[prefix + "macdhist3"] = macdhist

            if check_indicator(prefix + "slow"):
                slowk, slowd = talib.STOCH(high, low, close, fastk_period=period05, slowk_period=period, slowk_matype=0, slowd_period=period, slowd_matype=0)
                df[prefix + "slowk"] = slowk
                df[prefix + "slowd"] = slowd

            if check_indicator(prefix + "fast"):
                fastk, fastd = talib.STOCHF(high, low, close, fastk_period=period05, fastd_period=period, fastd_matype=0)
                df[prefix + "fastk"] = fastk
                df[prefix + "fastd"] = fastd

            if check_indicator(prefix + "fast"):
                fastk, fastd = talib.STOCHRSI(close, timeperiod=period, fastk_period=period05, fastd_period=period03, fastd_matype=0)
                df[prefix + "fastk2"] = fastk
                df[prefix + "fastd2"] = fastd

            #  __     _____  _    _   _ __  __ _____
            #  \ \   / / _ \| |  | | | |  \/  | ____|
            #   \ \ / / | | | |  | | | | |\/| |  _|
            #    \ V /| |_| | |__| |_| | |  | | |___
            #     \_/  \___/|_____\___/|_|  |_|_____|
            if check_indicator(prefix + "ADOSC"):            
                real = talib.ADOSC(high, low, close, volume, fastperiod=period03, slowperiod=period)
                df[prefix + "ADOSC"] = real

            #  __     _____  _        _  _____ ___ _     ___ _______   __
            #  \ \   / / _ \| |      / \|_   _|_ _| |   |_ _|_   _\ \ / /
            #   \ \ / / | | | |     / _ \ | |  | || |    | |  | |  \ V /
            #    \ V /| |_| | |___ / ___ \| |  | || |___ | |  | |   | |
            #     \_/  \___/|_____/_/   \_\_| |___|_____|___| |_|   |_|
            if check_indicator(prefix + "ATR"):            
                real = talib.ATR(high, low, close, timeperiod=period)
                df[prefix + "ATR"] = real

            if check_indicator(prefix + "NATR"):
                real = talib.NATR(high, low, close, timeperiod=period)
                df[prefix + "NATR"] = real

            #   ____ _____  _  _____ ___ ____ _____ ___ ____ ____
            #  / ___|_   _|/ \|_   _|_ _/ ___|_   _|_ _/ ___/ ___|
            #  \___ \ | | / _ \ | |  | |\___ \ | |  | | |   \___ \
            #   ___) || |/ ___ \| |  | | ___) || |  | | |___ ___) |
            #  |____/ |_/_/   \_\_| |___|____/ |_| |___\____|____/
            if check_indicator(prefix + "BETA"):            
                real = talib.BETA(high, low, timeperiod=period)
                df[prefix + "BETA"] = real

            if check_indicator(prefix + "CORREL"):
                real = talib.CORREL(high, low, timeperiod=period)
                df[prefix + "CORREL"] = real

            if check_indicator(prefix + "LINEARREG"):
                real = talib.LINEARREG(close, timeperiod=period)
                df[prefix + "LINEARREG"] = real

            if check_indicator(prefix + "LINEARREG_ANGLE"):
                real = talib.LINEARREG_ANGLE(close, timeperiod=period)
                df[prefix + "LINEARREG_ANGLE"] = real

            if check_indicator(prefix + "LINEARREG_INTERCEPT"):
                real = talib.LINEARREG_INTERCEPT(close, timeperiod=period)
                df[prefix + "LINEARREG_INTERCEPT"] = real

            if check_indicator(prefix + "LINEARREG_SLOPE"):
                real = talib.LINEARREG_SLOPE(close, timeperiod=period)
                df[prefix + "LINEARREG_SLOPE"] = real

            if check_indicator(prefix + "STDDEV"):
                real = talib.STDDEV(close, timeperiod=period, nbdev=1)
                df[prefix + "STDDEV"] = real

            if check_indicator(prefix + "TSF"):
                real = talib.TSF(close, timeperiod=period)
                df[prefix + "TSF"] = real

            if check_indicator(prefix + "VAR"):
                real = talib.VAR(close, timeperiod=period, nbdev=1)
                df[prefix + "VAR"] = real

            #   _____ ___ _   _ _____  _
            #  |  ___|_ _| \ | |_   _|/ \
            #  | |_   | ||  \| | | | / _ \
            #  |  _|  | || |\  | | |/ ___ \
            #  |_|   |___|_| \_| |_/_/   \_\
            set_cols()
            #if check_indicator_add("VWAP", prefix):
            #      real = fin_custom.VWAP(df, period=period)
            #      add("VWAP", real, prefix)
            #
            #if check_indicator_add("CFI", prefix):
            #      real = fin_custom.CFI(df, period=period)
            #      add("CFI", real, prefix)
            #
            #if check_indicator_add("VPT", prefix):
            #      real = fin_custom.VPT(df, period=period)
            #      add("VPT", real, prefix)
            #
            #if check_indicator_add("ADL", prefix):
            #      tmp = fin_custom.ADL(df, period=period)
            #      add("ADL", tmp, prefix)
            #
            #if check_indicator_add("OBV", prefix):
            #      tmp = fin_custom.OBV(df, period=period)
            #      add("OBV", tmp, prefix)
            
            if check_indicator_add("WMA", prefix):
                  real = fin.WMA(df, period=period)
                  add("WMA", real, prefix)
            
            if check_indicator_add("HMA", prefix):
                  real = fin.HMA(df, period=period)
                  add("HMA", real, prefix)
            
            if check_indicator_add("PPO", prefix):
                  real = fin.PPO(df, period_fast=period05, period_slow=period, signal=period03)
                  add("PPO", real, prefix)
            
            if check_indicator_add("EVWMA", prefix):
                  #real = fin.EVWMA(df, period=period)
                  #add("EVWMA", real, prefix)
                  pass
            
            if check_indicator_add("IFT_RSI", prefix):
                  real = fin.IFT_RSI(df, rsi_period=period05, wma_period=period)
                  add("IFT_RSI", real, prefix)
            
            if check_indicator_add("FVE", prefix):
                  real = fin.FVE(df, period=period)
                  add("FVE", real, prefix)
            
            if check_indicator_add("VFI", prefix):
                  real = fin.VFI(df, period=period)
                  add("VFI", real, prefix)
            
            if check_indicator_add("STC", prefix):
                  #real = fin.STC(df, period_fast=period05, period_slow=period, period=period02)
                  #add("STC", real, prefix)
                  pass
            
            if check_indicator_add("AO", prefix):
                  real = fin.AO(df, slow_period=period * 2, fast_period=period05)
                  add("AO", real, prefix)
            
            if check_indicator_add("MI", prefix):
                  real = fin.MI(df, period=period)
                  add("MI", real, prefix)
            
            if check_indicator_add("VORTEX", prefix):
                  real = fin.VORTEX(df, period=period)
                  add("VORTEX", real, prefix)
            
            if check_indicator_add("VZO", prefix):
                  real = fin.VZO(df, period=period)
                  add("VZO", real, prefix)
            
            if check_indicator_add("PZO", prefix):
                  real = fin.PZO(df, period=period)
                  add("PZO", real, prefix)
            
            if check_indicator_add("EFI", prefix):
                  real = fin.EFI(df, period=period)
                  add("EFI", real, prefix)
            
            if check_indicator_add("EMV", prefix):
                  real = fin.EMV(df, period=period)
                  add("EMV", real, prefix)
            
            if check_indicator_add("CCI", prefix):
                  real = fin.CCI(df, period=period)
                  add("CCI", real, prefix)
            
            if check_indicator_add("BASP", prefix):
                  real = fin.BASP(df, period=period)
                  add("BASP", real, prefix)
            
            if check_indicator_add("WTO", prefix):
                  real = fin.WTO(df, channel_lenght=period05, average_lenght=period)
                  add("WTO", real, prefix)
            
            if check_indicator_add("FISH", prefix):
                  real = fin.FISH(df, period=period)
                  add("FISH", real, prefix)
            
            if check_indicator_add("TSI", prefix):
                  tmp = fin.TSI(df, long=period, short=period05, signal=period05)
                  add("TSI", tmp, prefix)
            
            if check_indicator_add("MFI", prefix):
                  tmp = fin.MFI(df, period=period)
                  add("MFI", tmp, prefix)
            
            if check_indicator_add("ICHIMOKU", prefix):
                  tmp = fin.ICHIMOKU(df, tenkan_period=period02, kijun_period=period05, senkou_period=period, chikou_period=period05, )
                  add("ICHIMOKU", tmp, prefix)
            
            if check_indicator_add("APZ", prefix):
                  tmp = fin.APZ(df, period=period)
                  add("APZ", tmp, prefix)
            
            if check_indicator_add("SQZMI", prefix):
                  tmp = fin.SQZMI(df, period=period)
                  add("SQZMI", tmp, prefix)
            
            if check_indicator_add("KC", prefix):
                  tmp = fin.KC(df, period=period, atr_period=period05)
                  add("KC", tmp, prefix)
            
            if check_indicator_add("DO", prefix):
                  tmp = fin.DO(df, upper_period=period, lower_period=period03)
                  add("DO", tmp, prefix)
            
            if check_indicator_add("DMI", prefix):
                  tmp = fin.DMI(df, period=period)
                  add("DMI", tmp, prefix)

            #    ____ _   _ ____ _____ ___  __  __
            #   / ___| | | / ___|_   _/ _ \|  \/  |
            #  | |   | | | \___ \ | || | | | |\/| |
            #  | |___| |_| |___) || || |_| | |  | |
            #   \____|\___/|____/ |_| \___/|_|  |_|
            #if check_indicator_add("consolidation_zones", prefix):
            #      #tmp = cust.consolidation_zones(df, period, period05)
            #      #add("consolidation_zones", tmp, prefix)
            #      pass
            #
            #if check_indicator_add("trendflex", prefix):
            #      tmp = cust.trendflex(df, period, source="close")
            #      add("trendflex", tmp, prefix)
            #
            #if check_indicator_add("mesa_mama", prefix):
            #      tmp = cust.mesa_mama(df, fastLimit=0.25, slowLimit=0.05, warmUpPeriod=period)
            #      add("mesa_mama", tmp, prefix)
            #
            #if check_indicator_add("hurst_cycle", prefix):
            #      tmp = cust.hurst_cycle(df, scl_t=period03, mcl_t=period, scm=1, mcm=3, prefix="")
            #      add("hurst_cycle", tmp, prefix)
            #
            #if check_indicator_add("compute_Hc", prefix):
            #      tmp = cust.compute_Hc(close, kind="random_walk", min_window=period, max_window=None, simplified=True)
            #      add("compute_Hc", tmp, prefix)
            #
            #if check_indicator_add("fractal_dimension_index", prefix):
            #      tmp = cust.fractal_dimension_index(df, period)
            #      add("fractal_dimension_index", tmp, prefix)
            #
            #if check_indicator_add("donchian_channel", prefix):
            #      tmp = cust.donchian_channel(df, period, _min_periods=None, _fillna=True, _offset=0)
            #      add("donchian_channel", tmp, prefix)
            #
            #if check_indicator_add("fractal_sr", prefix):
            #      tmp = cust.fractal_sr(df, period)
            #      add("fractal_sr", tmp, prefix)
            #
            #if check_indicator_add("wavepm", prefix):
            #      tmp = cust.wave_pm(close, window=period)
            #      add("wavepm", tmp, prefix)
            #
            #if check_indicator_add("adr", prefix):
            #      tmp = cust.adr(df)
            #      add("adr", tmp, prefix)

        return df
