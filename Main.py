import os
import glob
import shutil
import pandas as pd

headers_dict = {
    1: "EMP",
    2: "TYPE",
    3: "UNITS",
    4: "HOURS",
    5: "DATE",
    6: "JOB",
    7: "PHASE",
    8: "CAT",
    9: "EQUIP_NUM",
    10: "EQUIP_CODE",
    11: "EQUIP_HRS",
    12: "WORK_TYPE",
    13: "COST_TYPE",
    14: "EQ_DATE"
}

default_data_types = {
    1: "TEXT",
    2: "INT",
    3: "FLOAT",
    4: "FLOAT",
    5: "DATE",
    6: "TEXT",
    7: "TEXT",
    8: "TEXT",
    9: "TEXT",
    10: "TEXT",
    11: "FLOAT",
    12: "INT",
    13: "INT",
    14: "DATE"
}


class FilesClass:
    """Can be used to find files, move files to a different folder, etc"""
    def __init__(self, folder_path: str):
        self.original_folder_path = folder_path

    def find_files(self, file_format: str) -> list or None:
        """Finds files based on the given file format"""
        self.file_format = file_format
        self.files_list = []
        self.csv_files_count = 0
        print(f"Looking for {self.file_format} in {self.original_folder_path}")
        self.path_and_file_format_str = fr"{self.original_folder_path}\*.{self.file_format}"
        for file in glob.glob(self.path_and_file_format_str):
            self.files_list.append(file)
            self.csv_files_count += 1
        if len(self.files_list) >= 1:
            return self.files_list
        else:
            return None

    def move_file_to_folder(self, destination_folder_path: str, file_name: str, file_extension: str):
        """Moves file to the given destination folder"""
        self.source_file = fr"{self.original_folder_path}\{file_name}.{file_extension}"
        self.destination_file = fr"{destination_folder_path}\{file_name}.{file_extension}"

        shutil.move(self.source_file, self.destination_file)

    @staticmethod
    def path_parser(path_with_file_name_and_extension: str) -> 'tuple(str, str, str)':
        """Breaks down a path into three parts (path, filename, and extension)"""
        complete_path = path_with_file_name_and_extension
        # Divide path into two parts
        path_only = os.path.dirname(complete_path)
        file_name_with_extension = os.path.basename(complete_path)
        # Divide file name into two parts
        filename, extension = os.path.splitext(file_name_with_extension)
        extension_without_period = extension.translate(str.maketrans('', '', '.'))
        return path_only, filename, extension_without_period


class CSVsClass(FilesClass):
    """Used to find and move CSV files"""
    def __init__(self, root_folder_path: str):
        super().__init__(root_folder_path)
        self.path = None
        self.file_name = None
        self.extension = None

    def find_one_file(self, *kwargs) -> str or None:
        """"Finds all the files in the given path but returns only one from the list"""
        file_list = super().find_files("csv")
        if file_list is not None:
            self.path, self.file_name, self.extension = super().path_parser(file_list[0])
            return file_list[0]
        else:
            return None

    def move_file(self, destination_folder_path: str):
        """Moves the file to the given destination path"""
        super().move_file_to_folder(destination_folder_path, self.file_name, "csv")


class DataFrameClass:
    """Used to perform all the necessary dataframe actions"""
    def __init__(self):
        self.requiredHeaders = [
            "EMP",
            "DATE",
            "JOB",
            "PHASE"
        ]
        self.original_file_path_only = None
        self.original_file_name_only = None
        self.original_file_extension_only = None

    def create_dataframe_without_headers(self, original_file_path_and_name: str) -> pd.DataFrame:
        """Creates dataframe without headers based on the data in the given CSV"""
        self.original_file_name = original_file_path_and_name
        self.original_dataframe = pd.read_csv(self.original_file_name, header=None, skiprows=1, skipinitialspace=True)
        return self.original_dataframe

    def delete_empty_rows(self, dataframe_with_empty_rows: pd.DataFrame) -> pd.DataFrame:
        """Deletes empty rows from a dataframe with the correct headers"""
        # gets rid of columns with empty records
        self.dataframe_without_empty_rows = dataframe_with_empty_rows
        self.dataframe_without_empty_rows.dropna(subset=self.requiredHeaders, inplace=True)
        return self.dataframe_without_empty_rows

    def delete_empty_columns(self, dataframe_with_empty_columns: pd.DataFrame) -> pd.DataFrame:
        """Deletes columns from a dataframe based on the number of headers"""
        self.dataframe_with_no_empty_columns = dataframe_with_empty_columns.copy()
        self.number_of_headers = len(headers_dict)
        self.dataframe_with_no_empty_columns.drop(self.dataframe_with_no_empty_columns.iloc[:, self.number_of_headers:],
                                                  inplace=True, axis=1)
        return self.dataframe_with_no_empty_columns

    def delete_extra_spaces(self, dataframe_with_extra_spaces: pd.DataFrame) -> pd.DataFrame:
        """Deletes leading and trailing empty spaces from strings"""
        self.dataframe_without_extra_spaces = dataframe_with_extra_spaces.copy()
        for key, value in default_data_types.items():
            if value is "TEXT":
                self.dataframe_without_extra_spaces[headers_dict[key]] = self.dataframe_without_extra_spaces[
                    headers_dict[key]].map(str.strip)
        return self.dataframe_without_extra_spaces

    def add_headers(self, dataframe_with_no_headers: pd.DataFrame) -> pd.DataFrame:
        """"Adds the correct headers to a dataframe without headers"""
        self.dataframe_with_headers = dataframe_with_no_headers.copy()
        self.dataframe_with_headers.columns = headers_dict.values()
        return self.dataframe_with_headers

    def add_extra_fields(self, dataframe_without_extra_fields: pd.DataFrame,
                         event_id: str, original_file_name_only: str) -> pd.DataFrame:
        """Adds extra fields to dataframe. Ex: EVENT_ID, ORIGINAL_FILE_NAME, etc"""
        self.dataframe_with_extra_fields = dataframe_without_extra_fields
        self.dataframe_with_extra_fields.insert(0, 'BATCH_ID', event_id)
        self.dataframe_with_extra_fields['ORIGINAL_FILE_NAME'] = original_file_name_only
        return self.dataframe_with_extra_fields

    def format_fields(self, dataframe_without_formatted_fields: pd.DataFrame) -> pd.DataFrame:
        """Removes null values and formats the dataframe's data based on data types"""
        self.df_with_format = dataframe_without_formatted_fields.copy()

        # TYPE has to be either 1 or 2
        self.df_with_format['TYPE'] = self.df_with_format['TYPE'].fillna(1)
        self.df_with_format['TYPE'] = self.df_with_format['TYPE'].astype(int)

        # COST_TYPE has to be a 1
        self.df_with_format['COST_TYPE'] = self.df_with_format['COST_TYPE'].fillna(1)
        self.df_with_format['COST_TYPE'] = self.df_with_format['COST_TYPE'].astype(int)

        for key, value in default_data_types.items():
            if value is "TEXT":
                self.df_with_format[headers_dict[key]] = self.df_with_format[headers_dict[key]].fillna('')
            elif value is "INT":
                self.df_with_format[headers_dict[key]] = self.df_with_format[headers_dict[key]].fillna(0)
                self.df_with_format[headers_dict[key]] = self.df_with_format[headers_dict[key]].astype(int)
            elif value is "FLOAT":
                self.df_with_format[headers_dict[key]] = self.df_with_format[headers_dict[key]].fillna(0)
                self.df_with_format[headers_dict[key]] = self.df_with_format[headers_dict[key]].astype(float)
            elif value is "DATE":
                self.df_with_format[headers_dict[key]] = pd.to_datetime(self.df_with_format[headers_dict[key]])

        return self.df_with_format

