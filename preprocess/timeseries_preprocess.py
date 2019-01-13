import pandas as pd


def time_series_construction(df, key_col='station_id', time_col='date_observed', value_col='value'):
    """
    Construct time series vector, indexed by distinct timestamp, columned by locations
    Using merge function to merge locations one by one
    If there is not timestamp at the location, filled with NaN as default

    :param df: input dataFrame
    :param key_col: column name of key
    :param time_col: column name of time
    :param value_col: column name of value
    :return: time series format data
    """
    time_idx = sorted(list(set(df[time_col])))
    time_series = pd.DataFrame(time_idx, columns=[time_col])
    time_series.set_index(time_col, inplace=True)

    df_grouped = df.groupby(key_col)
    for each_group in df_grouped.groups:
        group = df_grouped.get_group(each_group)
        sorted_group = group.sort_values(by=time_col)[[time_col, value_col]]
        sorted_group.set_index(time_col, inplace=True)
        time_series = time_series.join(sorted_group, how='left')
        time_series.rename(columns={value_col: each_group}, inplace=True)
    return time_series


def time_series_construction1(df, key_col='station_id', time_col='date_observed', value_col='value'):
    """
    An updated version from the first construction method
    Should be faster than the previous method

    :param df: input dataFrame
    :param key_col: column name of key
    :param time_col: column name of time
    :param value_col: column name of value
    :return: time series format data
    """
    min_time = min(df[time_col])
    max_time = max(df[time_col])
    times = pd.date_range(start=min_time, end=max_time, freq='1H')
    times_pd = pd.DataFrame(index=times)

    time_series_list = []
    # time_series_result = pd.DataFrame(index=times)

    df_grouped = df.groupby(key_col)
    for each_group in df_grouped.groups:
        group = df_grouped.get_group(each_group)
        group.set_index(time_col, inplace=True)
        time_series = times_pd.join(group[value_col], how='left')
        time_series.rename(columns={value_col: each_group}, inplace=True)
        time_series_list.append(time_series)
    time_series_result = pd.concat(time_series_list, axis=1)
    return time_series_result


def time_series_construction2(df, key_col='station_id', time_col='date_observed', value_col='value'):
    """
    Join operation takes too long if the column number is above 20.
    This will also takes sometime but short than join when column number is large

    :param df: input dataFrame
    :param key_col: column name of key
    :param time_col: column name of time
    :param value_col: column name of value
    :return: time series format data
    """
    min_time = min(df[time_col])
    max_time = max(df[time_col])
    times = pd.date_range(start=min_time, end=max_time, freq='1H')
    keys = list(set(df[key_col]))

    result_time_series = pd.DataFrame(index=times, columns=keys)

    for index, row in df.iterrows():
        station_id = row[key_col]
        time = row[time_col]
        result_time_series.loc[time, station_id] = row[value_col]
    return result_time_series


def time_series_smoothing(time_seires, window_size=24, method='mean'):
    """
        Smooth each time series (each column is a time series)

        :param time_seires: input dataFrame
        :param key_col: column name of key
        :param time_col: column name of time
        :param value_col: column name of value
        :return: time series format data
    """
    min_time = min(time_seires.index)
    max_time = max(time_seires.index)
    assert min_time < max_time

    times = pd.date_range(start=min_time, end=max_time, freq='1H')
    time_series_smooth = pd.DataFrame(index=times)

    if method == 'mean':
        for idx, key in enumerate(time_seires.columns):
            ts = time_seires[key]
            rolmean = ts.rolling(window=window_size, min_periods=1, center=True).mean()
            time_series_smooth[key] = rolmean

    if method == 'median':
        for idx, key in enumerate(time_seires.columns):
            ts = time_seires[key]
            rolmed = ts.rolling(window=window_size, min_periods=1, center=True).median()
            time_series_smooth[key] = rolmed

    return time_series_smooth
