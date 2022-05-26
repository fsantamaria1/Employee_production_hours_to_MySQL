import os
import glob
import shutil
import pandas as pd
import pymysql.cursors

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


class MySQLClass:
    """Used to check, create, and write records to the database as well as create tables"""
    def __init__(self, database_table_name: str):
        self.connection = pymysql.connect(host="host", port="port", user="user", passwd="password",
                                          db="database", charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.connection.cursor()
        self.table_name = database_table_name
        self.db_table_fields = {
            1: "ID",
            2: "BATCH_ID",
            3: "EMP",
            4: "TYPE",
            5: "UNITS",
            6: "HOURS",
            7: "DATE",
            8: "JOB",
            9: "PHASE",
            10: "CAT",
            11: "EQUIP_NUM",
            12: "EQUIP_CODE",
            13: "EQUIP_HRS",
            14: "WORK_TYPE",
            15: "COST_TYPE",
            16: "EQ_DATE",
            17: "ORIGINAL_FILE_NAME"
        }

        self.db_table_data_types = {
            1: "INT AUTO_INCREMENT PRIMARY KEY",
            2: "TEXT NOT NULL",
            3: "TEXT NOT NULL",
            4: "INTEGER NOT NULL",
            5: "FLOAT",
            6: "FLOAT",
            7: "DATE NOT NULL",
            8: "TEXT NOT NULL",
            9: "TEXT NOT NULL",
            10: "TEXT NOT NULL",
            11: "TEXT",
            12: "TEXT",
            13: "FLOAT",
            14: "INTEGER NOT NULL",
            15: "INTEGER NOT NULL",
            16: "DATE",
            17: "TEXT"
        }

    def check_database_records(self, dataframe_with_rows: pd.DataFrame) -> dict or None:
        """Checks if the database contains matching records based on the first row of the given dataframe"""
        self.data = dataframe_with_rows
        self.first_record = self.data.iloc[0]
        self.result = None
        # Copy of the data types dictionary which will contain everything but floats
        self.non_floats = {}
        # Iterate through the data types dictionary and copy all the keys and values skipping floats
        for key, value in default_data_types.items():
            if value is not "FLOAT":
                self.non_floats[key] = value
        # Starting select statement
        self.select_str = f"""SELECT *  FROM {self.table_name} WHERE """
        # Grab the last key from the dictionary
        last_key = list(self.non_floats.keys())[-1]
        # Loop through the non floats dictionary
        for key, value in self.non_floats.items():
            #  Append the matching header
            self.select_str += headers_dict[key]
            self.select_str += " = "
            # Append the matching row values from the database
            self.select_str += f"'{self.first_record[headers_dict[key]]}'"
            if key is not last_key:
                self.select_str += " AND "
            elif key is last_key:
                self.select_str += ";"
        print(self.select_str)
        # Assign result's batch ID to variable
        self.selected_results = self.cursor.execute(self.select_str)
        print("Number of results: ", self.selected_results)

        if self.selected_results > 0:
            self.result = self.cursor.fetchone()
            print("First result: ", self.result)
            print("BATCH_ID: ", self.result['BATCH_ID'])
            print("Result type: ", type(self.result))

        return self.result

    # not being used right now
    def retrieve_results_using_batch_id(self, batch_id: str) -> 'tuple(int, dict)':
        """Returns the number of results and all the results with the same batch_id"""
        self.batch_id = batch_id
        # Starting select statement
        self.select_str = f"""SELECT *  FROM {self.table_name} WHERE BATCH_ID = '{self.batch_id}'"""
        print("Select by BATCH_ID: ", self.select_str)

        self.results_by_batch_id = self.cursor.execute(self.select_str)
        print("Number of results by BATCH_ID: ", self.results_by_batch_id)
        self.all_the_results = self.cursor.fetchall()
        return self.results_by_batch_id, self.all_the_results

    def create_table_if_not_exists(self):
        """Creates a table if one with the same name does not exist"""

        # create string with fields and data types
        create_table_str = f"""CREATE TABLE IF NOT EXISTS {self.table_name} ("""
        last_key_number = list(self.db_table_fields.keys())[-1]
        for key, value in self.db_table_fields.items():
            create_table_str += value
            create_table_str += " "
            create_table_str += self.db_table_data_types[key]
            if key is not last_key_number:
                create_table_str += ", "
            if key is last_key_number:
                create_table_str += ")"

        print(create_table_str)
        self.cursor.execute(create_table_str)

    def write_records_to_database(self, final_dataframe_without_matching_database_records: pandas.DataFrame):
        """"Writes the records from the processed dataframe to the database"""
        self.df_insert = final_dataframe_without_matching_database_records
        # Will contain necessary fields
        self.fields = {}

        # Copy necessary fields
        for key, value in self.db_table_fields.items():
            # Skip the 1 key since it is an auto_increment primary key
            if key is not 1:
                self.fields[key] = value

        self.insert_statement = f"""INSERT INTO {self.table_name}"""

        self.last_field_key = list(self.fields.keys())[-1]
        self.first_field_key = list(self.fields.keys())[0]

        # Add fields to statement
        for key, value in self.fields.items():
            if key is self.first_field_key:
                self.insert_statement += " ("

            self.insert_statement += value

            if key is self.last_field_key:
                self.insert_statement += ") "
            else:
                self.insert_statement += ", "

        self.insert_statement += "VALUES"

        # After this loop, the statement will be completed
        for key in self.fields.keys():
            if key is self.first_field_key:
                self.insert_statement += " ("
            # Statement needs %s placeholders
            self.insert_statement += "%s"

            if key is self.last_field_key:
                self.insert_statement += ")"
            else:
                self.insert_statement += ","

        print(self.insert_statement)

        # LThis loop will add the actual values to the statement and insert them
        for index, row in self.df_insert.iterrows():
            # A tuple is needed to insert the values into the MySQL db
            list_of_row_values = ()
            for value in self.fields.values():
                list_of_row_values += (row[value],)
            print(list_of_row_values)
            self.cursor.executemany(self.insert_statement, [list_of_row_values])
        self.connection.escape_string(self.insert_statement)
        # If the connection is not committed, the values never get inserted into the database
        self.connection.commit()

