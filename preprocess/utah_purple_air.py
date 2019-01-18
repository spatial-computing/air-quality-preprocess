from data_model.air_model import AIRModel
from data_model.geo_model import GEOModel
from services.utils import check_max_min_correlation
from services.timeseries_preprocess import time_series_smoothing, time_series_construction1
from services.postgres_connection import Connection


def utah_purple_air_preprocess(feature_set, config):

    conn = Connection(host='jonsnow.usc.edu', database='air_quality_dev')
    remover = [16, 74, 76]

    air_quality_model = AIRModel(config, conn=conn)

    # NOTE: Utah PurpleAir data have duplicates for each pair [Station, Date_observed]
    # NOTE: 1. check the duplicates if the duplicates would effect (too different btw duplicates)
    max_min_corr_dic = check_max_min_correlation(air_quality_model.air_quality_df, air_quality_model.get_locations(),
                                                 key_col='station_id', time_col='date_observed')

    #       2. remove the stations that correlation of the values is low
    for k in max_min_corr_dic:
        if max_min_corr_dic[k] < 0.8:
            remover.append(k)
    air_quality_model.remove_stations(remover)

    #       3. take the mean of the duplicates for the rest of the stations
    air_quality_model.air_quality_df = air_quality_model.air_quality_df.groupby(['station_id', 'date_observed']).mean()
    air_quality_model.air_quality_df.reset_index(inplace=True)
    time_series_raw = time_series_construction1(air_quality_model.air_quality_df)

    assert time_series_raw is not None

    # TODO:Maybe add specific preprocssing way for PurpleAir data

    # NOTE: Smooth the time series
    air_quality_model.time_series = time_series_smoothing(time_series_raw)

    # NOTE: Get the geographic features
    geo_feature_model = GEOModel(air_quality_model.get_locations(), feature_set, config, conn=conn)

    conn.close_conn()
    print('Utah PurpleAir air quality data and geographic data construction finished.')
    return air_quality_model, geo_feature_model

