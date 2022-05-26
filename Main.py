import os
import glob
import shutil


class FilesClass:
    """Can be used to find files, move files to a different folder, etc"""

    # def __init__(self, folder_path=r"C:\Timesheets\Labor Hour Imports\CSVs"):
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


