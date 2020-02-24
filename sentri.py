import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from matplotlib import style
from tqdm import tqdm
style.use('fivethirtyeight')

df = pd.read_csv('datasets/GASBTC-5m-data.csv', index_col=0)
df.drop(columns=["quote_av", "close_time", "tb_base_av", "tb_quote_av", "ignore", "volume", "trades"], inplace=True)

# Get the lowest low for the last 5 values
df['low_24hr'] = df['low'].shift(1).rolling(window=288, center=False).min() #window=288 in acual

# Get Percentage change
def get_pct_change(high, low_24hr):
    pct_change = 100 * (high - low_24hr)/low_24hr
    return pct_change
df['pct_change'] = list(map(get_pct_change, df['high'], df['low_24hr']))
df.head()

# Make sure past window set doesnt have a very low pct_change
def check_24hr_low_pct(values):
    lowest_low_pct = min(values)
    if lowest_low_pct > -20:
        return True
    else:
        return False

df['low_24hr_check'] = df['pct_change'].shift(1).rolling(window=5, center=False).apply(func=check_24hr_low_pct)
df.tail()

# Stage 1 begins
df.reset_index(inplace=True)
df["fib_retracement"] = np.nan
df["candles_to_rise"] = np.nan
df.head()
len(df.index)


# 24hr change > 20%
# Point of the lowest_low itself should not have a 24_change less than -20%
# Price greater than 0.0000005
for row in tqdm(df.itertuples()):
    if row.pct_change > 20 and row.low_24hr_check == 1 and row.open > 0.0000005: # pct_change > 20 in actual
        candle_index = row.Index
        candle_high = row.high
        low_24hr = row.low_24hr
        fib_price_range = candle_high - low_24hr
        fib_bounce_threshold = 0.25 * fib_price_range # 25% is arbritrary can experiment with later.
        threshold_price = row.low_24hr + ((row.high - row.low_24hr) * 0.1)

        # Next 10 candles have highs lower than this candles high
        next_10_lower = True
        for i in range(candle_index + 1, candle_index + 11):
            a_iter_high = df.iloc[i]["high"]
            if a_iter_high >= candle_high:
                next_10_lower == False

        if next_10_lower == True:
            # Previous 10 candles have highs lower than this candles high
            previous_10_lower = True
            for j in range(candle_index - 1, candle_index - 11):
                b_iter_high = df.iloc[j]["high"]
                if b_iter_high >= candle_high:
                    previous_10_lower == False

            if previous_10_lower == True:
                # Get Number of candles between this candle and the threshold_candle
                # All these candles should have high less than high of this candle
                count = 1
                for k in range(candle_index - 1, -1, -1):
                    b_iter_high = df.iloc[k]["high"]
                    b_iter_open = df.iloc[k]["open"]
                    if b_iter_open < threshold_price: # threshold candle found
                        break
                    if b_iter_high < candle_high:
                        count += 1
                    else:
                        break

                # Scan the next 20 candles
                for l in range(candle_index + 1, candle_index + 21):
                    c_iter_index = l
                    c_iter_low = df.iloc[l]["low"]

                    # Get the lowest low and its index
                    if l == candle_index + 1:
                        lowest_low = c_iter_low
                        lowest_low_index = l
                    elif c_iter_low < lowest_low:
                        lowest_low = c_iter_low
                        lowest_low_index = l

                # Find if there is a bounce
                match_found = False
                for m in range(lowest_low_index + 1, lowest_low_index + 11):
                    d_iter_high = df.iloc[m]["high"]
                    price_bounce = d_iter_high - lowest_low
                    if price_bounce > fib_bounce_threshold:
                        match_found = True
                        break

                # Add the columns
                if match_found == True:
                    # Add the feature (no. of candles to rise)
                    df.iloc[candle_index]["candles_to_rise"] = count

                    price_diff = lowest_low - low_24hr
                    fib_retracement_label = price_diff / fib_price_range
                    df.iloc[candle_index]["fib_retracement"] = fib_retracement_label

df.to_csv('processed_datasets/GAS_processed.csv')

df.tail()
