import pandas as pd


class FlatFile(object):

    def __init__(self, file_path, time_column):

        # save the file path
        self._file_path = file_path
        # find the file type
        self._file_type = self._get_file_type(file_path)
        # load the flat file into a dataframe
        self._df = self._load_file()
        # set the time index
        self._set_index(time_column)

    @staticmethod
    def _get_file_type(file_path):
        file_ext = file_path.split(".")[-1]
        return file_ext

    def _load_file(self):
        if 'xls' in self._file_type:
            df = pd.read_excel(self._file_path)
        elif 'csv' in self._file_type:
            df = pd.read_csv(self._file_path)
        else:
            raise Exception('File type not supported yet')
        return df

    def _set_index(self, time_column):
        self._df.set_index(time_column, inplace=True)

    def get_his_series(self, column_name):
        return self._df[column_name]




