import numpy as np
import pandas as pd
import pickle
import os
import sys
import yaml


def time_series_data_constrution(df, key_col='station_id', time_col='date_observed', value_col='value'):
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

    min_time = min(df[time_col])
    max_time = max(df[time_col])
    times = pd.date_range(start=min_time, end=max_time, freq='1H')
    times_series = pd.DataFrame(index=times)

    df_grouped = df.groupby(key_col)
    cols = [times_series]
    for each_group in df_grouped.groups:
        group = df_grouped.get_group(each_group)
        sorted_group = group.sort_values(by=time_col)[[time_col, value_col]]
        sorted_group.set_index(time_col, inplace=True)
        sorted_group.rename(columns={'value': str(each_group)}, inplace=True)
        sorted_group = sorted_group[~sorted_group.index.duplicated(keep='first')]
        cols.append(sorted_group)
    return pd.concat(cols, axis=1, join='outer')


# simply replace nan values with 0
def replace_nan(x, y):
    x[np.isnan(x)] = 0
    y[np.isnan(y)] = 0
    return x, y


def split_by_chunk(data, chunk_size):
    chunks = []
    for i in range(chunk_size, len(data), chunk_size):
        chunks.append(data[i - chunk_size:i])
    last = data[i:] if i < len(data) else None
    return chunks, last


def split_train_val_test(chunk, train_size, val_size, test_size):
    if len(chunk) != train_size + val_size + test_size:
        return None
    return chunk[:train_size], chunk[train_size: train_size + val_size], chunk[-test_size:]


def generate_x_y_series_data(data, x_size, y_size):
    xs, ys = [], []
    x_indices = np.arange(x_size)
    y_indices = np.arange(x_size, x_size + y_size, 1)
    for i in range(0, len(data) - x_size - y_size + 1, 1):
        x = data[x_indices + i]
        y = data[y_indices + i]
        xs.append(x)
        ys.append(y)

    xs = np.array(xs)
    ys = np.array(ys)

    return xs, ys


def generate_yaml(input_yaml, output_path, x_window, y_window):
    with open(input_yaml, 'r') as stream:
        try:
            y = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    d = 'data/'+ os.path.basename(output_path)
    y['base_dir'] = d
    y['data']['dataset_dir'] = d
    y['model']['seq_len'] = x_window
    y['model']['horizon'] = y_window

    yaml_path = output_path+'/config.yaml'

    with open(yaml_path, 'w') as stream:
        yaml.dump(y, stream)

    print('yaml file saved at %s' % (yaml_path))


def generate_data_config_for_training(df, x_window=6, y_window=6, yaml_path=None, save_path=None,
                                      date_column='date_observed',
                                      split_rule='normal',
                                      **feature_args):
    npdata = np.expand_dims(df.drop(columns=[date_column]).values, axis=-1)
    npdata[np.isnan(npdata)] = 0

    # feature args
    if feature_args:
        pass
    if split_rule == '3week1week':
        total_week = 4 * 24 * 7
        train_week = round(3 * 24 * 7 * 0.875)
        test_week = 1 * 24 * 7
        val_week = total_week - train_week - test_week

        months, last = split_by_chunk(npdata, total_week)

        x_train, x_val, x_test = [], [], []
        y_train, y_val, y_test = [], [], []

        for mo in months:
            train, val, test = split_train_val_test(mo, train_week, val_week, test_week)
            _x_train, _y_train = generate_x_y_series_data(train, x_window, y_window)
            _x_val, _y_val = generate_x_y_series_data(val, x_window, y_window)
            _x_test, _y_test = generate_x_y_series_data(test, x_window, y_window)
            x_train.append(_x_train)
            x_val.append(_x_val)
            x_test.append(_x_test)
            y_train.append(_y_train)
            y_val.append(_y_val)
            y_test.append(_y_test)

        if last is not None:
            train, val = np.array_split(last, 2)
            _x_train, _y_train = generate_x_y_series_data(train, x_window, y_window)
            _x_val, _y_val = generate_x_y_series_data(val, x_window, y_window)
            x_train.append(_x_train)
            x_val.append(_x_val)
            y_train.append(_y_train)
            y_val.append(_y_val)

        x_train = np.concatenate(x_train)
        x_val = np.concatenate(x_val)
        x_test = np.concatenate(x_test)

        y_train = np.concatenate(y_train)
        y_val = np.concatenate(y_val)
        y_test = np.concatenate(y_test)

    # normal spliting method
    elif split_rule in {'normal', 'shuffle'}:
        x, y = generate_x_y_series_data(npdata, x_window, y_window)
        if split_rule == 'normal':
            pass
        elif split_rule == 'shuffle':
            shuffle_indices = np.random.permutation(x.shape[0])
            x = x[shuffle_indices]
            y = y[shuffle_indices]
        else:
            pass

        num_samples = len(x)
        num_test = round(num_samples * 0.2)
        num_train = round(num_samples * 0.7)
        num_val = num_samples - num_test - num_train

        # train
        x_train, y_train = x[:num_train], y[:num_train]

        # val
        x_val, y_val = (
            x[num_train: num_train + num_val],
            y[num_train: num_train + num_val],
        )
        # test
        x_test, y_test = x[-num_test:], y[-num_test:]
    else:
        print('no such split rule')
        sys.exit(1)

    x_offsets = np.sort(
        # np.concatenate(([-week_size + 1, -day_size + 1], np.arange(-11, 1, 1)))
        np.concatenate((np.arange(-x_window, 1, 1),))
    )

    y_offsets = np.sort(np.arange(1, 1 + y_window, 1))

    os.makedirs(os.path.dirname(os.path.join(save_path, "train.npz")), exist_ok=True)
    for cat in ["train", "val", "test"]:
        _x, _y = locals()["x_" + cat], locals()["y_" + cat]
        print(cat, "x: ", _x.shape, "y:", _y.shape)
        np.savez_compressed(
            os.path.join(save_path, "%s.npz" % cat),
            x=_x,
            y=_y,
            x_offsets=x_offsets.reshape(list(x_offsets.shape) + [1]),
            y_offsets=y_offsets.reshape(list(y_offsets.shape) + [1]),
        )

    # names = {}
    #
    # names['pred_dates'] = df[date_column][window:][-(num_test + window - 1): -(window - 1)]
    # names['sensor_ids'] = df.drop(columns=[date_column]).columns.values.tolist()
    # with open('names.dict', 'wb') as f:
    #     pickle.dump(names, f)
    generate_yaml(input_yaml=yaml_path, output_path=save_path, x_window=x_window, y_window=y_window)
    print()


def generate_utah_data():
    df = pd.read_csv("../data/utah_purplar_air_pm25.csv", parse_dates=["date_observed"], infer_datetime_format=True)
    df.index = df.iloc[:, -1]
    x, y = generate_data_config_for_training(df, window=6, save_path='../../DCRNN-master/data/UTAH-AQI_normal')

    x, y = generate_data_config_for_training(df, window=6, split_rule='3week1week',
                                             save_path='../../DCRNN-master/data/UTAH-AQI_3week1week')


from services.postgres_connection import Connection
from services import timeseries_preprocess


def generate_utah_data_from_database():
    # con= Connection(host='128.125.184.132', database='air_quality_dev')
    # df=con.read_as_dataframe(table_name='air_quality_data.utah_purple_air_ground_level_hourly', column_set=["station_id", "date_observed::TIMESTAMP WITHOUT TIME ZONE as date_observed", "value"], request_condition="where  date_observed >= '2017-11-01' and date_observed < '2018-06-01'")
    # import pickle
    # pickle.dump(df, open('utah_db_data_in_pd.pickle', 'wb'))

    df = pickle.load(open('utah_db_data_in_pd.pickle', 'rb'))
    df = time_series_data_constrution(df=df, key_col='station_id', time_col='date_observed', value_col='value')
    # pickle.dump(df, open('utah_db_data_in_ts.pickle', 'wb'))
    # df = pickle.load(open('utah_db_data_in_ts.pickle', 'rb'))
    # print(df.head())

    df = df[
        ['1', '2', '3', '10', '19', '20', '22', '23', '25', '26', '27', '28', '29', '30', '31', '32', '34', '35', '36',
         '37', '38', '40']]
    df.reset_index(inplace=True)
    pickle.dump(df, open('../../DCRNN-master/data/all_utah_data_df.pickle', 'wb'))
    # x, y = generate_x_y(df, window=6, save_path='../../DCRNN-master/data/UTAH-AQI_all_data', date_column='index')
    generate_data_config_for_training(df, x_window=24, y_window=6, split_rule='normal',
                                             save_path='../../DCRNN-master/data/UTAH-AQI_all_24_6_normal_split',
                                             date_column='index',
                                             yaml_path='../../DCRNN-master/data/dcrnn_utah_3week1week.yaml')

    generate_data_config_for_training(df, x_window=24, y_window=6, split_rule='shuffle',
                                             save_path='../../DCRNN-master/data/UTAH-AQI_all_24_6_shuffle_split',
                                             date_column='index',
                                             yaml_path='../../DCRNN-master/data/dcrnn_utah_3week1week.yaml')



    generate_data_config_for_training(df, x_window=24, y_window=6, split_rule='3week1week',
                                             save_path='../../DCRNN-master/data/UTAH-AQI_all_24_6_3week1week_split',
                                             date_column='index',
                                             yaml_path='../../DCRNN-master/data/dcrnn_utah_3week1week.yaml')
    generate_data_config_for_training(df, x_window=24, y_window=24, split_rule='3week1week',
                                             save_path='../../DCRNN-master/data/UTAH-AQI_all_24_24_3week1week_split',
                                             date_column='index',
                                             yaml_path='../../DCRNN-master/data/dcrnn_utah_3week1week.yaml')
    generate_data_config_for_training(df, x_window=6, y_window=24, split_rule='3week1week',
                                             save_path='../../DCRNN-master/data/UTAH-AQI_all_6_24_3week1week_split',
                                             date_column='index',
                                             yaml_path='../../DCRNN-master/data/dcrnn_utah_3week1week.yaml')


generate_utah_data_from_database()
