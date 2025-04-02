from typing import List
from urllib.error import HTTPError
from xml.etree import ElementTree
from repositories.fetch_data_repo import FetchDataRepo

class S3Service:
    bucket_url: str
    folder_name: str
    file_list: List[str] = []

    def __init__(self, bucket_url, folder_name):
        self.bucket_url = bucket_url
        self.folder_name = folder_name

    def get_file_list_from_s3(self):
        try:
            response = FetchDataRepo.fetch_from_url(self.bucket_url)

            if response.status_code == 200:
                xml_tree = ElementTree.fromstring(response.content)
                for content in xml_tree.findall(".//{http://s3.amazonaws.com/doc/2006-03-01/}Key"):
                    self.file_list.append(content.text.replace(f'{self.folder_name}/', ''))

        except HTTPError as e:
            print(f"Failed to fetch CSV file from S3: {e}")
        except Exception as e:
            print(f"Unexpected error occured: {e}")


    def download_files_from_s3(self, destination_file_path):
        try:
            for file_name in self.file_list:
                file_url = self.bucket_url + '/' + self.folder_name + '/' + file_name
                response = FetchDataRepo.fetch_from_url(file_url)

                if response.status_code == 200:
                    try:
                        with open(destination_file_path + file_name, "wb") as f:
                            f.write(response.content)
                    except Exception:
                        print('Failed to write files to local machine')
            
            return self.file_list

        except HTTPError as e:
            print(f"Failed to fetch CSV file from S3: {e}")
        except Exception as e:
            print(f"Unexpected error occured: {e}")