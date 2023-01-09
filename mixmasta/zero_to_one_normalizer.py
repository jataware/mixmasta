import numpy as np


def normalize_dataframe(dataframe):
    """
    This function accepts a dataframe in the canonical format
    and min/max normalizes each feature to between 0 to 1
    """
    dataframe_normalized = dataframe.copy(deep=True)
    features = dataframe.feature.unique()

    for f in features:
        feat = dataframe_normalized[dataframe_normalized["feature"] == f]
        dataframe_normalized.loc[feat.index, "value"] = normalize_data(feat["value"])
    return dataframe_normalized


def normalize_data(data):
    """
    This function takes in an array and performs 0 to 1 normalization on it.
    It is robust to NaN values and ignores them (leaves as NaN).
    """
    return (data - np.min(data)) / (np.max(data) - np.min(data))
