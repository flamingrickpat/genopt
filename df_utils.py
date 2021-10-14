import numpy as np


def normalize(df, col):
    df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
    return df

def normalize_np(arr):
    tmp = np.max(arr) - np.min(arr)
    if tmp == 0 or np.isnan(tmp) or np.isinf(tmp):
        tmp = 1
    return (arr - np.min(arr)) / tmp


def pd_dt_to_str(dt):
    res = str(dt).replace("/", "").replace(":", "-").replace(" ", "_")
    if "+" in res:
        res = res.split("+")[0]
    return res
