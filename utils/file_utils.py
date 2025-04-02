
from csv import reader as csv_reader, DictReader as csv_dict_reader
from typing import Optional, TextIO
import zipfile

class CsvFileHandler:
    folder_path: str
    file_stream: Optional[TextIO] = None
    file_reader: Optional[csv_reader] = None

    def __init__(self, folder_path: str):
        self.folder_path = folder_path

    def get_headers(self, file_name: str):
        if self.file_stream:
            self.close()
    
        self.file_stream = open(self.folder_path + file_name, "r", encoding="UTF8", newline="")
        self.file_reader = csv_reader(self.file_stream)

        for _ in range(1):
            headers = next(self.file_reader)
        
        self.close()

        return headers
    
    def open_new_reader(self, file_name: str):
        if self.file_stream:
            self.close()
    
        self.file_stream = open(self.folder_path + file_name, "r", encoding="UTF8", newline="")
        self.file_reader = csv_dict_reader(self.file_stream)
    
    def close(self):
        #  close the self-managed file stream
        if self.file_stream:
            self.file_stream.close()

    def extract_csv_from_zip(self, file_name):
        with zipfile.ZipFile(self.folder_path + '/' + file_name, "r") as zip_ref:
            file_list = zip_ref.namelist()
            for file in file_list:
                zip_ref.extract(file, self.folder_path + '/')