"""
@description: Data Object class to handle output transformations.
@author: Rithwik Babu
"""
import pandas as pd


class DataObject:
    def __init__(self, name, data):
        self.__name = name
        self.__data = data

    def to_df(self) -> pd.DataFrame:
        """Exports data to a pandas DataFrame.

        :return: pd.Dataframe
        """
        return self.__data

    def to_csv(self, file_name):
        """Exports data to an Excel file.

        :param file_name: The name of the output Excel file.
        :return: None
        """
        self.__data.to_csv(file_name, index=False)

    def to_xlsx(self, file_name):
        """Exports data to an Excel file.

        :param file_name: The name of the output Excel file.
        :return: None
        """
        self.__data.to_excel(file_name, index=False)

    def show(self, n=5):
        """Print the first n rows of the data.

        :param n: Number of rows to print.
        :return: None, prints the head of the data.
        """
        print(self.__name)
        print(self.__data.head(n))
