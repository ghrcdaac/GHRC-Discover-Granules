import logging
import os
import boto3

from bs4 import BeautifulSoup
import requests
import re
import botocore.exceptions
from dateutil.parser import parse

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        """
        Establishes a session for requests.
        """
        return self.session.get(url, verify=False)

    def html_request(self, url_path: str):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = self.fetch_session(url_path)
        return BeautifulSoup(opened_url.text, features="html.parser")

    def headers_request(self, url_path: str):
        """
        Performs a head request for the given url.
        :param url_path The URL for the request
        :return Results of the request
        """
        return self.session.head(url_path).headers

    def upload_to_s3(self, granule_dict: dict):
        """
        Upload a file to an S3 bucket
        :param granule_dict: List of granules to be written to S3
        """
        temp_str = ""
        for key, value in granule_dict.items():
            temp_str += f"{str(key)},{value.get('ETag')},{value.get('Last-Modified')}\n"
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
            logging.debug(nk)
            return {}

        return granule_dict

    def get_headers(self, granule):
        """
        Gets the ETag and Last-Modified fields from a head response and returns it as a dictionary
        :param granule The url to request the header for
        :return temp a dictionary with {"key": {"ETag": "ETag", "Last-Modified": "Last-Modified"}}
        """
        head_resp = self.headers_request(granule)
        temp = {granule: {}}
        temp[granule]['ETag'] = str(head_resp.get('ETag', None))
        last_modified = head_resp.get('Last-Modified', None)
        if isinstance(last_modified, str):
            temp[granule]['Last-Modified'] = str(parse(last_modified))

        return temp

    @staticmethod
    def error(granule_dict, s3_granule_dict):
        """
        If the "error" flag is set in the collection definition this function will throw an exception and halt
        execution.
        :param granule_dict granules discovered this run
        :param s3_granule_dict the downloaded last run stored in s3
        :return new_granules Only the granules that are newly discovered
        """
        new_granules = {}
        for key, value in granule_dict.items():
            if key in s3_granule_dict:
                raise ValueError(f"A duplicate granule was found: {key}")
            else:
                # Update for S3
                s3_granule_dict[key] = {}
                s3_granule_dict[key]["ETag"] = granule_dict[key]["ETag"]
                s3_granule_dict[key]["Last-Modified"] = granule_dict[key]["Last-Modified"]
                # Dictionary for new or updated granules
                new_granules[key] = {}
                new_granules[key]["ETag"] = granule_dict[key]["ETag"]
                new_granules[key]["Last-Modified"] = granule_dict[key]["Last-Modified"]

        return new_granules

    @staticmethod
    def skip(granule_dict, s3_granule_dict):
        """
        If the skip flag is set in the collection definition this function will only update granules if the ETag or
        Last-Modified meta-data tags have changed.
        :param granule_dict granules discovered this run
        :param s3_granule_dict the downloaded last run stored in s3
        :return new_granules Only the granules that are newly discovered
        """
        new_granules = {}
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
                s3_granule_dict[key]["ETag"] = granule_dict[key]['ETag']
                s3_granule_dict[key]['Last-Modified'] = granule_dict[key]['Last-Modified']
                is_new_or_modified = True

            if is_new_or_modified:
                new_granules[key] = {}
                new_granules[key]["ETag"] = granule_dict[key]['ETag']
                new_granules[key]['Last-Modified'] = granule_dict[key]['Last-Modified']

        return new_granules

    @staticmethod
    def replace(granule_dict, s3_granule_dict):
        """
         If the replace flag is set in the collection definition this function will clear out the previously stored run
         and replace with any discovered granules for this run.
         :param granule_dict granules discovered this run
         :param s3_granule_dict the downloaded last run stored in s3
         :return new_granules Only the granules that are newly discovered
         """
        s3_granule_dict.clear()
        for key, value in granule_dict.items():
            s3_granule_dict[key] = {}
            s3_granule_dict[key]["ETag"] = granule_dict[key]['ETag']
            s3_granule_dict[key]['Last-Modified'] = granule_dict[key]['Last-Modified']

        return s3_granule_dict

    def check_granule_updates(self, granule_dict: {}, duplicates="skip"):
        """
        Checks stored granules and updates the datetime and ETag if updated
        :param granule_dict: Dictionary of granules to check
        :param duplicates Variable for telling the code how to handle when a duplicate granule is discovered
         - skip: If we discovered a granule already discovered, only update it if the ETag or Last-Modified have changed
         - error: if we discover a granule already discovered, throw and error and terminate execution
         - replace: If we discovered a granule already discovered, update it anyways
        :return Dictionary of granules that were new or updated
        """
        s3_granule_dict = self.download_from_s3()
        new_or_updated_granules = getattr(self, duplicates)(granule_dict, s3_granule_dict)

        # Only re-upload if there were new or updated granules
        if new_or_updated_granules:
            self.upload_to_s3(s3_granule_dict)
        return new_or_updated_granules

    def discover_granules_http(self, url_path, file_reg_ex=None, dir_reg_ex=None, depth=0):
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
        depth = int(depth)
        fetched_html = self.html_request(url_path)
        directory_list = []
        for a_tag in fetched_html.findAll('a', href=True):
            url_segment = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
            path = f"{url_path.rstrip('/')}/{url_segment}"
            head_resp = self.headers_request(path)
            etag = head_resp.get('ETag')
            last_modified = head_resp.get('Last-Modified')

            if (etag is not None or last_modified is not None) and \
                    (file_reg_ex is None or re.search(file_reg_ex, url_segment)):
                granule_dict[path] = {}
                granule_dict[path]['ETag'] = str(etag)
                # The isinstance check is needed to prevent unit tests from trying to parse a MagicMock
                # object which will cause a crash
                if isinstance(head_resp.get('Last-Modified'), str):
                    granule_dict[path]['Last-Modified'] = str(parse(last_modified).timestamp())

            elif dir_reg_ex is None or re.search(dir_reg_ex, path):
                directory_list.append(f"{path}/")
            else:
                logging.debug(f"Notice: {path} not processed as granule or directory.")
        pass

        depth = min(abs(depth), 3)
        if depth > 0:
            for directory in directory_list:
                granule_dict.update(
                    self.discover_granules_http(url_path=directory, file_reg_ex=file_reg_ex,
                                                dir_reg_ex=dir_reg_ex, depth=(depth - 1))
                )

        return granule_dict

    @staticmethod
    def discover_granules_s3(host: str, prefix: str, file_reg_ex=None, dir_reg_ex=None):
        """
        Fetch the link of the granules in the host s3 bucket
        :param host: The bucket where the files are served
        :param prefix: The path for the s3 granule
        :param file_reg_ex: Regular expression used to filter files
        :param dir_reg_ex: Regular expression used to filter directories
        :return: links of files matching reg_ex (if reg_ex is defined)
        """
        s3_client = boto3.client('s3')
        s3_paginator = s3_client.get_paginator('list_objects')

        response_iterator = s3_paginator.paginate(
            Bucket=host,
            Prefix=prefix,
            PaginationConfig={
                'PageSize': 1000
            }
        )

        ret_dict = {}
        for page in response_iterator:
            for s3_object in page['Contents']:
                key = s3_object['Key']
                sections = str(key).rsplit('/', 1)
                key_dir = sections[0]
                file_name = sections[1]
                if (file_reg_ex is None or re.search(file_reg_ex, file_name)) and \
                        (dir_reg_ex is None or re.search(dir_reg_ex, key_dir)):
                    ret_dict[key] = {
                        'ETag': s3_object['ETag'],
                        'Last-Modified': s3_object['LastModified'].timestamp()
                    }

        return ret_dict
