import numpy as np
import pandas


def scale_dataframe(dataframe):
    """
    This function accepts a dataframe in the canonical format
    and min/max scales each feature to between 0 to 1
    """
    dfs = []
    features = dataframe.feature.unique()

    for f in features:
        feat = dataframe[dataframe["feature"] == f].copy()
        feat["value"] = scale_data(feat["value"])
        dfs.append(feat)
    return pandas.concat(dfs)


def scale_data(data):
    """
    This function takes in an array and performs 0 to 1 normalization on it.
    It is robust to NaN values and ignores them (leaves as NaN).
    """
    return (data - np.min(data)) / (np.max(data) - np.min(data))
