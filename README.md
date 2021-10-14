# genopt

I made this script 2 years ago when I started getting into algotrading.
Basically, I tried to brute force a good strategy to use in freqtrade. 
It works like this:

1. Select a random ticker from Binance
2. Download a random range with 4000 - 6000 candles
3. Make a pandas dataframe and fill it with a lot of indicators
4. Categorize all new columns (ma, oscillator)
5. Generate a statement from random indicators and operators
6. Apply the statement to get the buy signal
7. Backtest the signals from the random indicators
8. Write the results like win rate, sharpe ratio, sortino ratio to sqlite database

A statement could look like this:
```
(df["200_fastk2"] < df["300_ROCR100"]) & (df["7_MFI"].shift(98) > df["30_MINUS_DI"].shift(169)) & (df["100_MFI"] < df["40_ROCR100"]) & (df["125_PLUS_DM"].shift(65) < 0.0016316869429259797) & (df["9_fastk2"] < df["30_ADX"].shift(58))
```
I gave up when I found out about overfitting. 
It was an interesting experiment, but pretty useless in the end. Maybe this can safe someone work if they have the same idea as me.

## Usage
```
python main.py config.json
```

## Configuration
You can change the timeframes, tickers and data range in config.json.