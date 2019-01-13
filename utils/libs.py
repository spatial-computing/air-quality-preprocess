from sklearn import preprocessing


class StandardScaler2:
    """
        Standard the whole input data
    """

    def __init__(self, mean, std):
        self.mean = mean
        self.std = std

    def transform(self, data):
        return (data - self.mean) / self.std

    def inverse_transform(self, data):
        return (data * self.std) + self.mean


def standard_scaler(df):
    """
        Standardize each column
    """

    scl = preprocessing.StandardScaler().fit(df)
    return scl.transform(df)
