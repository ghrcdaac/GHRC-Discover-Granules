import logging
import os
import boto3

from bs4 import BeautifulSoup
import requests
import re
import botocore.exceptions
from dateutil.parser import parse


class DiscoverGranules:
    """
    This class contains functions that fetch
    The metadata of the granules via a protocol X (HTTP/SFTP/S3)
    Compare the md5 of these granules with the ones in an S3
    It will return the files if they don't exist in S3 or the md5 doesn't match
    """

    def __init__(self, csv_file_name='granules.csv'):
        """
        Default values goes here
        """
        self.csv_file_name = csv_file_name
        self.s3_key = f"{os.getenv('s3_key_prefix', default='temp').rstrip('/')}/{self.csv_file_name}"
        self.s3_bucket_name = os.getenv("bucket_name")
        self.session = requests.Session()

    def fetch_session(self, url):
        return self.session.get(url)

    def html_request(self, url_path: str):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = self.fetch_session(url_path)
        return BeautifulSoup(opened_url.text, features="html.parser")

    def headers_request(self, url_path: str):
        return self.session.head(url_path).headers

    def upload_to_s3(self, granule_dict: dict):
        """
        Upload a file to an S3 bucket
        :param granule_dict: List of granules to be written to S3
        """
        temp_str = ""
        for key, value in granule_dict.items():
            temp_str += f"{str(key)},{value['ETag']},{value['Last-Modified']}\n"
        temp_str = temp_str[:-1]

        client = boto3.client('s3')
        client.put_object(Bucket=self.s3_bucket_name, Key=self.s3_key, Body=temp_str)

    def download_from_s3(self):
        """
        Download a file from an S3 bucket
        :return: Dictionary of the granules
        """
        granule_dict = {}
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.s3_bucket_name)

        try:
            obj = bucket.Object(key=self.s3_key)
            response = obj.get()

            lines = response['Body'].read().decode('utf-8').split('\n')
            for row in lines:
                values = str(row).split(',')
                granule_dict[values[0]] = {}
                granule_dict[values[0]]['ETag'] = values[1]
                granule_dict[values[0]]['Last-Modified'] = values[2]

        except botocore.exceptions.ClientError as nk:
            logging.error(nk)
            return {}

        return granule_dict

    def check_granule_updates(self, granule_dict: {}):
        """
        Checks stored granules and updates the datetime and ETag if updated
        :param granule_dict: Dictionary of granules to check
        :return Dictionary of granules that were new or updated
        """
        new_or_updated_granules = {}
        s3_granule_dict = self.download_from_s3()
        for key, value in granule_dict.items():
            is_new_or_modified = False
            if key in s3_granule_dict:
                if s3_granule_dict[key]['ETag'] != granule_dict[key]['ETag']:
                    s3_granule_dict[key]['ETag'] = granule_dict[key]['ETag']
                    is_new_or_modified = True
                if s3_granule_dict[key]['Last-Modified'] != granule_dict[key]['Last-Modified']:
                    s3_granule_dict[key]['Last-Modified'] = granule_dict[key]['Last-Modified']
                    is_new_or_modified = True

            else:
                s3_granule_dict[key] = {}
                s3_granule_dict[key]['ETag'] = granule_dict[key]['ETag']
                s3_granule_dict[key]['Last-Modified'] = granule_dict[key]['Last-Modified']
                is_new_or_modified = True

            if is_new_or_modified:
                new_or_updated_granules[key] = {}
                new_or_updated_granules[key]['ETag'] = granule_dict[key]['ETag']
                new_or_updated_granules[key]['Last-Modified'] = granule_dict[key]['Last-Modified']

        # Only re-upload if there were new or updated granules
        if new_or_updated_granules:
            self.upload_to_s3(s3_granule_dict)
        return new_or_updated_granules

    def get_file_links_http(self, url_path, file_reg_ex=None, dir_reg_ex=None, depth=0):
        """
        Fetch the link of the granules in the host url_path
        :param url_path: The base URL where the files are served
        :type url_path: string
        :param file_reg_ex: Regular expression used to filter files
        :type file_reg_ex: string
        :param dir_reg_ex: Regular expression used to filter directories
        :param depth: The positive number of levels to search down, will use the lesser of 3 or depth
        :return: links of files matching reg_ex (if reg_ex is defined)
        :rtype: dictionary of urls
        """
        granule_dict = {}

        try:
            depth = int(depth)
            fetched_html = self.html_request(url_path)
            directory_list = []
            for a_tag in fetched_html.findAll('a', href=True):
                url_segment = a_tag['href'].rstrip('/').rsplit('/', 1)[-1]
                path = f"{url_path}{url_segment}"
                '''
                Checking for a '.' here to see the link that has been discovered is a file. 
                This assumes that a discovered file will have an appended portion ie file.txt
                Notice it is only checking the newest discovered portion of the URL.
                '''

                if '.' in url_segment and (file_reg_ex is None or re.search(file_reg_ex, url_segment)):
                    head_resp = self.headers_request(path)
                    granule_dict[path] = {}
                    granule_dict[path]['ETag'] = str(head_resp.get('ETag'))
                    # This check is needed to prevent unit tests from trying to parse a MagicMock object which crashes
                    if isinstance(head_resp.get('Last-Modified'), str):
                        granule_dict[path]['Last-Modified'] = str(parse(head_resp.get('Last-Modified')))

                elif dir_reg_ex is None or re.search(dir_reg_ex, path):
                    directory_list.append(f"{path}/")
            pass

            depth = min(abs(depth), 3)
            if depth > 0:
                for directory in directory_list:
                    granule_dict.update(
                        self.get_file_links_http(url_path=directory, file_reg_ex=file_reg_ex,
                                                 dir_reg_ex=dir_reg_ex, depth=(depth - 1))
                    )
        except ValueError as ve:
            logging.error(ve)

        return granule_dict
