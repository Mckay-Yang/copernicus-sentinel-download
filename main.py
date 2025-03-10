import os
import time
import requests
import inspect
import threading
import hashlib
from datetime import datetime
import pandas as pd
from urllib.parse import quote

class SentinelDownloader:
    def __init__(
        self, url: str,
        payload: dict,
        user_name: str,
        password: str,
        proxy: str,
        download_path: str,
    ):
        self.url = url.strip()
        self.payload = payload
        self.user_name = user_name
        self.password = password
        self.proxy = {
            'http': proxy,
            'https': proxy,
        }
        self.download_path = download_path
        self._token = self._get_token()
        self._download_list = self._get_download_list()
        self._thread_num = 4 # The maximum number of threads supported by the copericus server is 4
        self._retry = 10
        self._breakpoint_retry_flag = False # breakpoint download is not supported by the copericus server

    def _get_token(self) -> str:
        data = {
            'client_id': 'cdse-public',
            'username': self.user_name,
            'password': self.password,
            'grant_type': 'password',
        }
        while True:
            try:
                r = requests.post(
                    'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token',
                    data=data, proxies=self.proxy
                )
                self._log('Token achieve success.')
                return r.json()['access_token']
            except Exception as e:
                self._log_err(f'Error[{e}] occurred when getting the access token')
                self._log_err('Access token creation failed. waiting for 5s to get token...')
                time.sleep(5)

    def _update_token(self):
        self._token = self._get_token()

    def _get_download_list(self) -> list:
        while True:
            try:
                r = requests.get(
                    self.url,
                    headers={'Authorization': f'Bearer {self._token}'},
                    params=self.payload,
                    proxies=self.proxy
                )
                ret = r.json()
                if 'value' in ret and len(ret['value']) > 0:
                    self._log('Get download list success.')
                    return ret['value']
                elif 'value' in ret and len(ret['value']) == 0:
                    self._log_err(f'No data found\n {ret}')
                    return []
                elif 'detail' in ret:
                    raise Exception(ret['detail'])
                else:
                    raise Exception(f'Unknown error {ret}')
            except Exception as e:
                self._log_err(f'Error[{e}] occurred when getting the download list')
                self._log_err('Waiting for 5s to get download list...')
                time.sleep(5)

    def download(self):
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        sem = threading.Semaphore(self._thread_num)
        for item in self._download_list:
            sem.acquire()
            t = threading.Thread(target=self._download_thread, args=(item, sem))
            t.start()

    def _download_thread(self, item: dict, sem: threading.Semaphore):
        for i in range(self._retry):
            try:
                self._download_item(item)
                break
            except Exception as e:
                self._log(f'Retry {i + 1} times to download {item["Name"]}')
                time.sleep(1)
                if i == self._retry - 1:
                    self._log_err(f'Download {item["Name"]} failed.')
        sem.release()

    def _download_item(self, item: dict):
        try:
            id = item['Id']
            download_url = f'https://download.dataspace.copernicus.eu/odata/v1/Products({id})/$value'
            file_name = item['Name']
            file_path = os.path.join(self.download_path, file_name + '.zip')
            md5_sum = item['Checksum'][0]['Value']
            headers={
                'Authorization': f'Bearer {self._token}',
                # 'Accept': 'application/json, text/plain, */*',
                # 'accept-encoding': 'gzip, deflate, br, zstd',
                # 'accept-language': 'en,zh-CN;q=0.9,zh;q=0.8,ja;q=0.7',
                # 'origin': 'https://browser.dataspace.copernicus.eu',
                # 'priority': 'u=1, i',
                # 'referer': 'https://browser.dataspace.copernicus.eu/',
                # 'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
                # 'sec-ch-ua-mobile': '?0',
                # 'sec-ch-ua-platform': '"Windows"',
                # 'sec-fetch-dest': 'empty',
                # 'sec-fetch-mode': 'cors',
                # 'sec-fetch-site': 'same-site',
                # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
                #     '(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            }
            if os.path.exists(file_path):
                if hashlib.md5(open(file_path, 'rb').read()).hexdigest() == md5_sum:
                    self._log(f'File {file_name} already exists and MD5 check passed, skip downloading.')
                    return # After return will run the finally block, so the f.close() couldn't in finnally block
                if self._breakpoint_retry_flag:
                    self._log(f'File {file_name} already exists, resume downloading.')
                    file_size = os.path.getsize(file_path)
                    headers['Range'] = f'bytes={file_size}-'
                else:
                    self._log(f'File {file_name} already exists, but MD5 check failed, retry downloading.')
                    os.remove(file_path)
            r = requests.get(
                url=download_url,
                headers=headers,
                proxies=self.proxy,
                stream=True,
                timeout=60)
            self._log(f'Start downloading file {file_name}.')
            if r.status_code != 200 and r.status_code != 206:
                err_msg = r.json()
                if err_msg['detail'] == 'Expired signature!':
                    self._update_token()
                    raise Exception('Token expired')
                self._log_err(f'Error[{r.status_code}{err_msg}] ourcced when downloading file {file_name}.')
                raise Exception(f'Downloading file {file_name} failed.')
            with open(file_path, "ab") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if(chunk):
                        f.write(chunk)
            if hashlib.md5(open(file_path, 'rb').read()).hexdigest() != md5_sum:
                self._log_err(f'MD5 check failed when downloading file {file_name}, abort file and retry.')
                os.remove(file_path)
                raise Exception('Data Error')
            self._log(f'Download file {file_name} success.')
            r.close()
        except Exception as e:
            self._log_err(f'Error[{e}] ourcced when downloading file {file_name}.')
            r.close()
            raise Exception(f'Downloading file {file_name} failed.')

    def _log(self, msg):
        if not os.path.exists('./log'):
            os.makedirs('./log')
        with open('./log/log.log', 'a') as f:
            function_name = inspect.currentframe().f_back.f_code.co_name
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f'[{timestamp}] [{function_name}]: {msg}\n')

    def _log_err(self, msg):
        if not os.path.exists('./log'):
            os.makedirs('./log')
        with open('./log/err.log', 'a') as f:
            function_name = inspect.currentframe().f_back.f_code.co_name
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f'[{timestamp}] [{function_name}]: {msg}\n')


if __name__ == '__main__':
    user_name = 'username' # Your username
    password = 'password' # Your password
    proxy = '127.0.0.1:7890' # If you don't need proxy, set it to None
    path = './download' # The path to save the downloaded data
    collection = 'SENTINEL-2' # The collection name
    level = 'L2A' # The level of the data
    start_time = '2015-01-01'
    end_time   = '2018-01-04'
    aoi = '((92.86 27.99, 97.1 27.99, 97.1 31.05, 92.86 31.05, 92.86 27.99))' # The area of interest, try to use the smallest area as possible
    asc_download = True

    t_start = pd.to_datetime(start_time)
    t_end = t_start + pd.DateOffset(months=1) # Download data by month
    aoi_url = aoi_url = quote(aoi, safe='(),;')
    if asc_download:
        download_sequence = 'asc'
    else:
        download_sequence = 'desc'

    while t_start < pd.to_datetime(end_time):
        if t_end > pd.to_datetime(end_time):
            t_end = pd.to_datetime(end_time)

        t_start_str = t_start.strftime('%Y-%m-%dT%H:%M:%S')
        t_end_str = t_end.strftime('%Y-%m-%dT%H:%M:%S')
        # Construct the url and payload
        url = f'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-2%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27MSI%27)%20and%20(contains(Name,%27{level}%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20{aoi_url}%27)))%20and%20Online%20eq%20true)%20and%20ContentDate/Start%20ge%20{t_start_str}Z%20and%20ContentDate/Start%20lt%20{t_end_str}Z)&$orderby=ContentDate/Start%20{download_sequence}&$expand=Attributes&$count=True&$top=1000&$expand=Assets&$skip=0'
        payload = {'$filter': f'''((Collection/Name eq '{collection}' and (Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'instrumentShortName' and att/OData.CSC.StringAttribute/Value eq 'MSI') and (contains(Name,'{level}') and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON {aoi}'))) and Online eq true) and ContentDate/Start ge {t_start_str}Z and ContentDate/Start lt {t_end_str}Z)''',
            '$orderby': f'ContentDate/Start {download_sequence}',
            '$expand': 'Attributes',
            '$count': 'True',
            '$top': '1000',
            '$expand': 'Assets',
            '$skip': '0'
        }

        # Download data
        downloader = SentinelDownloader(url, payload, user_name, password, proxy, path)
        print(f'Downloading data from {t_start} to {t_end}')
        downloader.download()

        t_start = t_end
        t_end = t_start + pd.DateOffset(months=1)

    print('Download success, thanks for using')
