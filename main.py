from demo import cross_validation, validating_prediction, fine_scale_prediction, interpolation
from services.common_postgres import Connection
from model.geo_model import GEOModel
from services.helpers import read_json
from utils.constants import *

import pandas as pd
import time
import os


def main():

    """
        Program settings
    """
    pd.set_option('precision', 15)
    conn = Connection(host='jonsnow.usc.edu', database='air_quality_dev')

    file_path = 'data/model/model_config.json'
    config = read_json(file_path)

    data_file_path = config['data_path']
    data_config = read_json(data_file_path)
    config = dict(config, **data_config)

    # NOTE: Add the current time in the config for file writing
    config['time'] = int(time.time())
    config['output_path'] = get_file_path(config)
    parameter_name = config['parameter_name']

    # NOTE: Remove the stations because they are repeated/duplicated if they have
    remover = []
    if parameter_name == 'pm25':
        remover = [1, 16, 3, 14, 19, 38, 20, 35, 20, 48, 51, 52, 53, 74, 76]
    elif parameter_name == 'pm10':
        remover = ['E San Gabriel V-1', 'SW Coastal LA', 'NW Coastal LA', 'E San Fernando Vly']
    elif parameter_name == 'o3':
        remover = ['S San Gabriel Vly', 'E San Gabriel V-1', 'E San Fernando Vly']
    else:
        exit(1)

    # NOTE: Get the EPA air quality data and geographic data
    air_quality_model = air_quality_preprocessing("training", remover, config, conn)
    geo_feature_model = GEOModel(air_quality_model.air_quality_locations, config['training_geo_feature_table_name_pr'],
                                 config['training_additional_geo_feature'], config, conn)

    if config['testing_method'] == 'cv':
        cross_validation.prediction(air_quality_model, geo_feature_model, config)

    elif config['testing_method'] == 'cv_idw':
        interpolation.prediction(air_quality_model, config, conn)

    elif config['testing_method'] == 'validation':
        validating_prediction.prediction(air_quality_model, geo_feature_model, config, conn)

    elif config['testing_method'] == 'validation_idw':
        interpolation.prediction(air_quality_model, config, conn)

    elif config['testing_method'] == 'fishnet':
        fine_scale_prediction.prediction(air_quality_model, geo_feature_model,
                                         la_fishnet_geofeature_tablename, config, conn)


def get_file_path(config):
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


if __name__ == "__main__":
    main()
