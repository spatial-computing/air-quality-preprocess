from data_model.air_model import AIRModel
from data_model.geo_model import GEOModel
from services.timeseries_preprocess import time_series_smoothing
from services.postgres_connection import Connection


def los_angeles_ppa_preprocess(feature_set, config):

    conn = Connection(host='jonsnow.usc.edu', database='air_quality_dev')
    parameter_name = config['parameter_name']

    air_quality_model = AIRModel(config, conn=conn)
    locations = air_quality_model.air_quality_locations

    # NOTE: Remove the stations because they are repeated/duplicated if they have
    remover = []
    if parameter_name == 'pm25':
        remover = []
    elif parameter_name == 'pm10':
        remover = []
    elif parameter_name == 'o3':
        remover = []
    else:
        exit(1)

    """
        Maybe add specific preprocssing way for PurpleAir data
        
    """
    # e.g., Smoothing the time series
    air_quality_model.air_quality_time_series = time_series_smoothing(air_quality_model.air_quality_time_series)

    # NOTE: Remove some repeated stations
    air_quality_model.air_quality_locations = [x for x in locations if x not in remover]
    air_quality_model.air_quality_time_series = \
        air_quality_model.air_quality_time_series[air_quality_model.air_quality_locations]

    geo_feature_model = GEOModel(locations, feature_set, config, conn=conn)

    conn.close_conn()
    print('PurpleAir air quality data and geographic data construction finished.')
    return air_quality_model, geo_feature_model
