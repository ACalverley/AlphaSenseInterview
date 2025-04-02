
import os
from argparse import ArgumentParser
from services.s3_service import S3Service
from utils.file_utils import CsvFileHandler


def get_args():
    parser = ArgumentParser()

    parser.add_argument('--bucket_name', dest='bucket_name', type=str, help='Bucket name', required=True)
    parser.add_argument('--region', dest='region', type=str, help='Bucket region', required=True)
    parser.add_argument('--folder_name', dest='folder_name', type=str, help='Bucket data folder name', required=True)

    # Parse all arguments
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    bucket_name = args.bucket_name
    region = args.region
    folder_name = args.folder_name
    bucket_url = f"https://{bucket_name}.s3.{region}.amazonaws.com"

    current_working_directory = os.getcwd()
    local_file_path = current_working_directory + '/output/'

    s3_service = S3Service(bucket_url=bucket_url, folder_name=folder_name)
    s3_service.get_file_list_from_s3()

    file_list = s3_service.download_files_from_s3(local_file_path)
    
    csv_handler = CsvFileHandler(local_file_path)
    for file in file_list:
        csv_handler.extract_csv_from_zip(file)

    file_list_csv = []
    for zip_file in file_list:
        file_list_csv.append(zip_file.replace('.zip', '.csv'))

    # Question 1
    headers = csv_handler.get_headers('MNZIRS0108.csv')
    csv_handler.open_new_reader('MNZIRS0108.csv')
    for row in csv_handler.file_reader:
        if row['id'] == "MO_BS_INV":
            value = float(row['scale']) * float(row['2014-10-01'])
            print(f"\n\nQuestion 1: {value}")

    csv_handler.close()


    # Question 2


if __name__ == '__main__':
    main()