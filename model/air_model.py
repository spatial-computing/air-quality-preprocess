from preprocess.timeseries_preprocess import time_series_smoothing, time_series_construction1
from utils.lib import StandardScaler2

import numpy as np
import pandas as pd


class AIRModel:

    def __init__(self, config, conn, key_col='station_id', time_col='date_observed', value_col='value'):
        self._config = config['air_quality']

        self._key_col = key_col
        self._time_col = time_col
        self._value_col = value_col

        self._air_quality_df = self._get_air_quality(conn)
        self._clearned_air_quality_df = self._df_simple_cleaning(self._air_quality_df)
        self.air_quality_locations = self._get_locations()

        time_series = time_series_construction1(self._clearned_air_quality_df)
        if self._config['data_source'] == 'purple_air':
            self.air_quality_time_series = time_series_smoothing(time_series)
        if self._config['data_source'] == 'epa':
            self.air_quality_time_series = time_series

        assert self.air_quality_time_series is not None

        self.scaler = StandardScaler2(
            mean=self.air_quality_time_series.values[~np.isnan(self.air_quality_time_series.values)].mean(),
            std=self.air_quality_time_series.values[~np.isnan(self.air_quality_time_series.values)].std())

    def _df_simple_cleaning(self, df):
        """
        Construct air quality data to a DataFrame, columned by ['location', 'timestamp', 'value']
        Step1: Filter out the duplicates
        Step2: Filter out all negative values

        :param df: input data
        :return: a simply cleaned DataFrame
        """
        cleaned_df = df.drop_duplicates()
        cleaned_df = cleaned_df[cleaned_df[self._value_col] > 0.0]
        return cleaned_df

    def _get_locations(self):
        """
        Get distinct locations

        :return: a list of air quality locations
        """
        locations = self._clearned_air_quality_df[self._key_col].drop_duplicates()
        return list(locations)

    def _get_air_quality(self, conn):

        table_name = self._config['table_name']
        column_set = self._config['column_set']
        request_condition = self._config['request_condition']

        air_quality_data = conn.read(table_name, column_set, request_condition)
        air_quality_df = pd.DataFrame(air_quality_data, columns=['station_id', 'date_observed', 'value'])
        return air_quality_df
