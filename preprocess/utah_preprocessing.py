from model.air_model import AIRModel

import pandas as pd


def purple_air_preprocess(config, conn):

    parameter_name = config['parameter_name']
    air_quality_table_name = config['training_air_quality_table_name']
    air_quality_column_set = config['training_air_quality_column_set']
    air_quality_request_condition = config['training_air_quality_request_condition']

    sql = 'select {column} from {table_name} {condition};'\
        .format(column=','.join(air_quality_column_set), table_name=air_quality_table_name,
                condition=air_quality_request_condition)

    air_quality_data = conn.execute_wi_return(sql)
    air_quality_df = pd.DataFrame(air_quality_data, columns=['station_id', 'date_observed', 'value'])
    air_quality_model = AIRModel(air_quality_df, config)

    # NOTE: Remove the stations because they are repeated/duplicated if they have
    remover = []
    if parameter_name == 'pm25':
        remover = [65, 67, 68, 'E San Gabriel V-1', 'SW Coastal LA', 'NW Coastal LA', 'E San Fernando Vly', 'Antelope Vly']
    elif parameter_name == 'pm10':
        remover = ['E San Gabriel V-1', 'SW Coastal LA', 'NW Coastal LA', 'E San Fernando Vly']
    elif parameter_name == 'o3':
        remover = ['S San Gabriel Vly', 'E San Gabriel V-1', 'E San Fernando Vly']
    else:
        exit(1)

    air_quality_location = air_quality_model.air_quality_locations

    # NOTE: Remove some repeated stations
    air_quality_model.air_quality_locations = [x for x in air_quality_location if x not in remover]
    air_quality_model.air_quality_time_series = \
        air_quality_model.air_quality_time_series[air_quality_model.air_quality_locations]

    print('EPA data construction finished.')
    return air_quality_model
