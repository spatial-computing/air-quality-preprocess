from sklearn import preprocessing
import json
import time
import os


def standard_scaler(df):
    scl = preprocessing.StandardScaler().fit(df)
    return scl.transform(df)


def check_max_min_correlation(df, stations, key_col, time_col):
    """
        Check the correlation of the duplicates at the same location & time

    :param df
    :param stations
    :param key_col
    :param time_col
    """

    df_min = df.groupby([key_col, time_col]).min()
    df_max = df.groupby([key_col, time_col]).max()
    joint_min_max = df_min.join(df_max, lsuffix='_min', rsuffix='_max')
    max_min_corr = {}
    for station in stations:
        corr = joint_min_max.loc[station].corr(method='pearson')
        max_min_corr[station] = corr.iloc[0, 1]
    return max_min_corr
