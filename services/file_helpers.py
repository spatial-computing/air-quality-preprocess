import math
import json


def read_json(file_path):
    try:
        json_data = open(file_path).read()
        data = json.loads(json_data)
        return data
    except IOError:
        print('Fail to load {} json file.'.format(file_path))
        return False


def write_csv(data, file_path):
    try:
        data.to_csv(file_path, header=True, index=True, sep=',', mode='w')
        return True
    except IOError:
        print('Fail to write csv file.')
        return False


