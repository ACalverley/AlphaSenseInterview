import requests

class FetchDataRepo:
    @staticmethod
    def fetch_from_url(bucket_url):
        response = requests.get(url=bucket_url, stream=False)
        response.raise_for_status()
        return response