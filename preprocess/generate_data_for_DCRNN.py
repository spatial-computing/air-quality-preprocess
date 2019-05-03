import numpy as np
import pandas as pd
import pickle
import os


# filter out samples with too many missing values
def filter_6(x, y):
    indice = []
    for i in range(x.shape[0]):
        x_count = np.count_nonzero(~np.isnan(x[i]))
        y_count = np.count_nonzero(~np.isnan(y[i]))
        # print(x_count)
        if x_count + y_count >= 210:
            indice.append(i)
    return x[indice], y[indice]


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


def generate_x_y(df, window=1, save_path=None, date_column='date_observed', split_rule=None,
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
            _x_train, _y_train = generate_x_y_series_data(train, window, window)
            _x_val, _y_val = generate_x_y_series_data(val, window, window)
            _x_test, _y_test = generate_x_y_series_data(test, window, window)
            x_train.append(_x_train)
            x_val.append(_x_val)
            x_test.append(_x_test)
            y_train.append(_y_train)
            y_val.append(_y_val)
            y_test.append(_y_test)

        if last is not None:
            train, val = np.split(last, 2)
            _x_train, _y_train = generate_x_y_series_data(train, window, window)
            _x_val, _y_val = generate_x_y_series_data(val, window, window)
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



        # previous version of 3week1week, there are overlapping bettween training, val, testing sets
        # x_train, x_val, x_test = [], [], []
        # y_train, y_val, y_test = [], [], []
        # for i in range(total_week, x.shape[0], total_week):
        #     month_x, month_y = x[i - total_week: i ], y[i - total_week: i ]
        #
        #     x_train.append(month_x[:train_week])
        #     x_val.append(month_x[train_week: train_week+val_week])
        #     x_test.append(month_x[-test_week:])
        #
        #     y_train.append( month_y[:train_week])
        #     y_val.append( month_y[train_week: train_week+val_week])
        #     y_test.append( month_y[-test_week:] )
        #
        # month_x, month_y = x[i:], y[i:]
        # x_train.append(month_x)
        # y_train.append(month_y)
        #
        # x_train = np.concatenate(x_train)
        # x_val= np.concatenate(x_val)
        # x_test= np.concatenate(x_test)
        #
        # y_train = np.concatenate(y_train)
        # y_val= np.concatenate(y_val)
        # y_test= np.concatenate(y_test)

    # normal processing method
    else:
        xdf = npdata[:-window]
        ydf = npdata[window:]

        indice = np.arange(window)
        x, y = [], []
        for i in range(xdf.shape[0] - window + 1):
            x.append(xdf[i + indice])
            y.append(ydf[i + indice])
        x, y = np.array(x), np.array(y)

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

    x_offsets = np.sort(
        # np.concatenate(([-week_size + 1, -day_size + 1], np.arange(-11, 1, 1)))
        np.concatenate((np.arange(-window, 1, 1),))
    )

    y_offsets = np.sort(np.arange(1, 1 + window, 1))

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

    names = {}

    names['pred_dates'] = df[date_column][window:][-(num_test + window - 1): -(window - 1)]
    names['sensor_ids'] = df.drop(columns=[date_column]).columns.values.tolist()
    with open('names.dict', 'wb') as f:
        pickle.dump(names, f)

    return x, y


def generate_utah_data():
    df = pd.read_csv("../data/utah_purplar_air_pm25.csv", parse_dates=["date_observed"], infer_datetime_format=True)
    df.index = df.iloc[:, -1]
    x, y = generate_x_y(df, window=6, save_path='../../DCRNN-master/data/UTAH-AQI_normal')

    x, y = generate_x_y(df, window=6, split_rule='3week1week', save_path='../../DCRNN-master/data/UTAH-AQI_3week1week')


generate_utah_data()
