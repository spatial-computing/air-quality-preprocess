import time
import json
import os

from preprocess.utah_epa import utah_epa_preprocess
from preprocess.utah_purple_air import utah_purple_air_preprocess


def load_config(file_path):
    json_data1 = open(file_path).read()
    config = json.loads(json_data1)

    data_file_path = config['data_config']
    json_data2 = open(data_file_path).read()
    data_config = json.loads(json_data2)
    config = dict(config, **data_config)

    # NOTE: Add the current time in the config
    config['time'] = int(time.time())

    # NOTE: Add output path in the config
    config['output_path'] = get_output_file_path(config)
    return config


def get_output_file_path(config):
    output_path = config['result_path']
    study_area = config['study_area']
    study_time = config['study_time']
    testing_method = config['testing_method']
    parameter_name = config['parameter_name']
    geo_feature_percent = str(config['geo_feature_percent'])
    output_path = output_path + '_'.join([study_area, study_time, testing_method, parameter_name, geo_feature_percent])
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    return output_path


def output_csv_file(file_nmae, air_quality_model):
    output_data = air_quality_model.time_series
    output_data['date_observed'] = output_data.index
    output_data.to_csv(file_nmae, header=True, index=False, sep=',', mode='w')


def test_los_angeles_epa():
    pass


def test_los_angeles_purple_air():
    pass


def test_utah_epa(write_file=True):
    config = load_config('../data/config/utah_model_config.json')
    air_quality_model, geo_feature_model = utah_epa_preprocess(config['feature_set'], config['testing'])
    assert air_quality_model is not None and geo_feature_model is not None

    if write_file:
        output_csv_file('../data/config/utah_epa_air_pm25', air_quality_model)


def test_utah_purple_air(write_file=True):
    config = load_config('../data/config/utah_model_config.json')
    air_quality_model, geo_feature_model = utah_purple_air_preprocess(config['feature_set'], config['training'])
    assert air_quality_model is not None and geo_feature_model is not None

    if write_file:
        output_csv_file('../data/config/utah_epa_air_pm25', air_quality_model)
