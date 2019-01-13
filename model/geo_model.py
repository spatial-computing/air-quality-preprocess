from utils.lib import *

import pandas as pd


class GEOModel:

    def __init__(self, locations, feature_set, config, conn):
        self._config = config['geo_feature']
        self._geo_feature_set = feature_set

        geo_feature_table_name_dic = self._get_geo_feature_table_name(self._config['table_name_pr'])

        self._column_set = self._config['column_set']
        self._gid_col = self._column_set[0]
        self._geo_feature_col = self._column_set[1]
        self._feature_type_col = self._column_set[2]
        self._buffer_size_col = self._column_set[3]
        self._value_col = self._column_set[4]

        self._geo_feature_df = self._get_geo_feature(locations, geo_feature_table_name_dic, conn)

        self.geo_feature_vector = self._constructing_feature_vector(locations)
        self.geo_feature_name = self._get_feature_name()
        self.scaled_geo_feature_vector = self._scaling_feature_vector(locations)

    def _get_geo_feature_table_name(self, geo_feature_table_name_pr):
        geo_feature_table_name = {}
        for geo_feature in self._geo_feature_set:
            geo_feature_table_name[geo_feature] = geo_feature_table_name_pr + '_' + geo_feature
        return geo_feature_table_name

    def _get_feature_name(self):
        """
        Get geographic feature types from the index of geo_feature_vector

        :return: A list of geographic feature type names
        """
        geo_feature_name = self.geo_feature_vector.index
        return list(geo_feature_name)

    def _scaling_feature_vector(self, locations):
        """
        Get a scaled geographic feature vector, scaled by column

        :param locations: selected location
        :return: Scaled geographic feature vector
        """
        scaled_geo_feature_vector = standard_scaler(self.geo_feature_vector.T)
        scaled_geo_feature_vector = pd.DataFrame(scaled_geo_feature_vector.T,
                                                 index=self.geo_feature_name, columns=locations)
        return scaled_geo_feature_vector

    def _constructing_feature_vector(self, locations):
        """
        Construct the feature vector, indexed by distinct geo features, columned by locations

        :param locations: selected location
        :return: a DataFrame columned (available) station name, indexed by geographic feature types
        """

        df_grouped = self._geo_feature_df.groupby(self._gid_col)

        self._geo_feature_df['feature_name'] = self._geo_feature_df[self._geo_feature_col] + '_' \
                                               + self._geo_feature_df[self._feature_type_col] + '_' \
                                               + self._geo_feature_df[self._buffer_size_col].map(str)
        feature_vector_list = []

        for each_group in df_grouped.groups:
            group = df_grouped.get_group(each_group)
            series = pd.Series(data=group[self._value_col].values, index=group['feature_name'], name=each_group)
            feature_vector_list.append(series)

        feature_vector = pd.concat(feature_vector_list, axis=1)

        # NOTE: In case there is no geographic features around certain locations, set value = 0.0
        for each_loc in locations:
            if each_loc not in feature_vector.columns:
                feature_vector[each_loc] = np.nan

        feature_vector = feature_vector.fillna(0.0)
        feature_vector = feature_vector[locations]
        print('Geographic feature vector construction finished.')
        return feature_vector

    def _get_geo_feature(self, locations, geo_feature_table_name_dic, conn):
        """
        Get all the geographic features for all locations from database

        :param locations: selected location
        :param geo_feature_table_name_dic: {geo_feature: related_table_name}
        :param conn: database connection
        :return: a DataFrame columned ["id", "geo_feature", "feature_type", "buffer_size", "value"]
        """

        additional_features = self._config['additional_features']
        geo_feature_df_list = []

        for geo_feature in self._geo_feature_set:
            this_geo_feature_table_name = geo_feature_table_name_dic[geo_feature]
            geo_feature_data = conn.read(this_geo_feature_table_name, self._column_set)
            geo_feature_df = pd.DataFrame(geo_feature_data, columns=self._column_set)
            # NOTE: filter the location based on the provided air quality locations
            geo_feature_df = geo_feature_df.loc[geo_feature_df[self._gid_col].isin(locations)]
            geo_feature_df_list.append(geo_feature_df)

        for geo_feature in additional_features.keys():
            this_geo_feature_table_name = additional_features[geo_feature]['table_name']
            column_list = additional_features[geo_feature]['column_set']
            column_set = ['{} as '.format(column_list[0]) + self._gid_col,
                          '\'location\' as ' + self._geo_feature_col,
                          '\'{}\' as '.format(geo_feature) + self._feature_type_col,
                          '0 as ' + self._buffer_size_col,
                          '{} as '.format(column_list[1]) + self._value_col]

            geo_feature_data = conn.read(this_geo_feature_table_name, column_set)
            geo_feature_df = pd.DataFrame(geo_feature_data, columns=self._column_set)
            # NOTE: filter the location based on the provided air quality locations
            geo_feature_df = geo_feature_df.loc[geo_feature_df[self._gid_col].isin(locations)]
            geo_feature_df_list.append(geo_feature_df)

        all_geo_feature_df = pd.concat(geo_feature_df_list)
        return all_geo_feature_df
