import concurrent.futures
import json
import logging
import os
import re
import time
from time import sleep

import boto3
import botocore
import botocore.exceptions
import requests
import urllib3
from bs4 import BeautifulSoup
from dateutil.parser import parse

from task.dgm import *

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DiscoverGranules:
    """
    This class contains functions that fetch
    The metadata of the granules via a protocol X (HTTP/SFTP/S3)
    Compare the md5 of these granules with the ones in an S3
    It will return the files if they don't exist in S3 or the md5 doesn't match
    """

    def __init__(self, event):
        """
        Default values goes here
        """
        self.config = event.get('config')
        self.provider = self.config.get('provider')
        self.collection = self.config.get('collection')
        self.discover_tf = self.collection.get('meta').get('discover_tf')
        csv_filename = f'{self.collection["name"]}__{self.collection["version"]}.csv'
        self.db_key = f'{os.getenv("s3_key_prefix", default="temp").rstrip("/")}/{DB_FILENAME}'
        self.s3_key = f'{os.getenv("s3_key_prefix", default="temp").rstrip("/")}/{csv_filename}'
        self.s3_bucket_name = os.getenv('bucket_name')
        self.s3_client = boto3.client('s3')
        self.session = requests.Session()
        table_resource = boto3.resource('dynamodb', region_name='us-west-2')
        self.db_table = table_resource.Table(os.getenv('table_name', default='DiscoverGranulesLock'))

    def discover(self):
        """
        Helper function to kick off the entire discover process
        """
        granule_dict = self.discover_granules()
        self.check_granule_updates_db(granule_dict)
        logging.info(f'Discovered {len(granule_dict)} granules.')
        output = self.cumulus_output_generator(granule_dict)
        self.write_db_file()
        self.db_file_cleanup()
        return {'granules': output}

    @staticmethod
    def populate_dict(target_dict, key, etag, last_mod):
        """
        Helper function to populate a dictionary with ETag and Last-Modified fields.
        Clarifying Note: This function works by exploiting the mutability of dictionaries
        :param target_dict: Dictionary to add a sub-dictionary to
        :param key: Value that will function as the new dictionary element key
        :param etag: The value of the ETag retrieved from the provider server
        :param last_mod: The value of the Last-Modified value retrieved from the provider server
        """
        target_dict[key] = {
            'ETag': str(etag),
            'Last-Modified': str(last_mod)
        }

    @staticmethod
    def update_etag_lm(dict1, dict2, key):
        """
        Helper function to update the Etag and Last-Modified fields when comparing two dictionaries.
        Clarifying Note: This function works by exploiting the mutability of dictionaries
        :param dict1: The dictionary to be updated
        :param dict2: The source dictionary
        :param key: The key of the entry to be updated
        """
        dict1[key] = {
            'ETag': dict2.get(key).get('ETag'),
            'Last-Modified': dict2.get(key).get('Last-Modified')
        }

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
        return BeautifulSoup(opened_url.text, features='html.parser')

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
        temp_str = "Name,ETag,Last-Modified (epoch)\n"
        for key, value in granule_dict.items():
            temp_str += f'{key},{value.get("ETag")},{value.get("Last-Modified")}\n'
        temp_str = temp_str[:-1]

        self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=self.s3_key, Body=temp_str)

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
            lines.pop(0)
            for row in lines:
                values = str(row).split(',')
                self.populate_dict(granule_dict, values[0], values[1], values[2])

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

    def error(self, granule_dict, s3_granule_dict):
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
                raise ValueError(f'A duplicate granule was found: {key}')
            else:
                # Update for S3
                self.update_etag_lm(s3_granule_dict, granule_dict, key)
                # Dictionary for new or updated granules
                self.update_etag_lm(new_granules, granule_dict, key)

        return new_granules

    def skip(self, granule_dict, s3_granule_dict):
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
            # if the key exists in the s3 dict, update it and add to new_granules
            if key in s3_granule_dict:
                if s3_granule_dict[key] != granule_dict[key]:
                    self.update_etag_lm(s3_granule_dict, granule_dict, key)
                    is_new_or_modified = True
            else:
                # else just add it to the s3 dict and new granules
                self.update_etag_lm(s3_granule_dict, granule_dict, key)
                is_new_or_modified = True

            if is_new_or_modified:
                self.update_etag_lm(new_granules, granule_dict, key)

        return new_granules

    @staticmethod
    def replace(granule_dict: {}, s3_granule_dict=None):
        """
         If the replace flag is set in the collection definition this function will clear out the previously stored run
         and replace with any discovered granules for this run.
         :param granule_dict granules discovered this run
         :param s3_granule_dict the downloaded last run stored in s3
         :return new_granules Only the granules that are newly discovered
         """
        if s3_granule_dict is None:
            s3_granule_dict = {}
        s3_granule_dict.clear()
        s3_granule_dict.update(granule_dict)
        return s3_granule_dict

    def check_granule_updates(self, granule_dict: {}):
        """
        Checks stored granules and updates the datetime and ETag if updated. Expected values for duplicateHandling are
        error, replace, or skip
        :param granule_dict: Dictionary of granules to check
        :return Dictionary of granules that were new or updated
        """
        duplicates = str(self.collection.get('duplicateHandling', 'skip')).lower()
        # TODO: This is a temporary work around to resolve the issue with updated RSS granules not being reingested.
        if duplicates == 'replace':
            duplicates = 'skip'

        getattr(self, f'{duplicates}')(granule_dict)

    def check_granule_updates_db(self, granule_dict: {}):
        """
        Checks stored granules and updates the datetime and ETag if updated. Expected values for duplicateHandling are
        error, replace, or skip
        :param granule_dict: Dictionary of granules to check
        :return Dictionary of granules that were new or updated
        """
        duplicates = str(self.collection.get('duplicateHandling', 'skip')).lower()
        # TODO: This is a temporary work around to resolve the issue with updated RSS granules not being reingested.
        if duplicates == 'replace':
            duplicates = 'skip'

        self.read_db_file()
        getattr(Granule, f'db_{duplicates}')(Granule, granule_dict)

    def discover_granules(self):
        """
        Function to be called to trigger the granule discover process once the class has been initialized with the
        correct cumulus event
        """
        return getattr(self, f'prep_{self.provider["protocol"]}')()

    def prep_s3(self):
        """
        Extracts the appropriate information for discovering granules using the S3 protocol
        """
        return self.discover_granules_s3(host=self.provider['host'], prefix=self.collection['meta']['provider_path'],
                                         file_reg_ex=self.collection.get('granuleIdExtraction'),
                                         dir_reg_ex=self.discover_tf.get('dir_reg_ex'))

    def prep_https(self):
        """
        Constructs an https url from the event provided at initialization and calls the http discovery function
        """
        return self.prep_http()

    def prep_http(self):
        """
        Constructs an http url from the event provided at initialization and calls the http discovery function
        """
        path = f'{self.provider["protocol"]}://{self.provider["host"].rstrip("/")}/' \
               f'{self.config["provider_path"].lstrip("/")}'
        return self.discover_granules_http(path, file_reg_ex=self.collection.get('granuleIdExtraction'),
                                           dir_reg_ex=self.discover_tf.get('dir_reg_ex'),
                                           depth=self.discover_tf.get('depth'))

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
            path = f'{url_path.rstrip("/")}/{url_segment}'
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
            elif (etag is None and last_modified is None) and \
                    (dir_reg_ex is None or re.search(dir_reg_ex, path)):
                directory_list.append(f'{path}/')
            else:
                logging.debug(f'Notice: {path} not processed as granule or directory.')
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
    def get_s3_resp_iterator(host, prefix, s3_client):
        """
        Returns an s3 paginator.
        :param host: The bucket.
        :param prefix: The path for the s3 granules.
        :param s3_client: S3 client to create paginator with.
        """
        s3_paginator = s3_client.get_paginator('list_objects')
        return s3_paginator.paginate(
            Bucket=host,
            Prefix=prefix,
            PaginationConfig={
                'PageSize': 1000
            }
        )

    def discover_granules_s3(self, host: str, prefix: str, file_reg_ex=None, dir_reg_ex=None):
        """
        Fetch the link of the granules in the host s3 bucket.
        :param host: The bucket where the files are served.
        :param prefix: The path for the s3 granule.
        :param file_reg_ex: Regular expression used to filter files.
        :param dir_reg_ex: Regular expression used to filter directories.
        :return: links of files matching reg_ex (if reg_ex is defined).
        """
        response_iterator = self.get_s3_resp_iterator(host, prefix, self.s3_client)

        ret_dict = {}
        for page in response_iterator:
            for s3_object in page.get('Contents'):
                key = s3_object['Key']
                sections = str(key).rsplit('/', 1)
                key_dir = sections[0]
                file_name = sections[1]
                if (file_reg_ex is None or re.search(file_reg_ex, file_name)) and \
                        (dir_reg_ex is None or re.search(dir_reg_ex, key_dir)):
                    self.populate_dict(ret_dict, key, s3_object['ETag'], s3_object['LastModified'].timestamp())

        return ret_dict

    @staticmethod
    def get_s3_filename(filename: str):
        """
        Helper function to prevent having to check the protocol for each file name assignment when generating the
        cumulus output.
        :param filename: In the case of granules discovered in S3 the entire key of the file has to be stored otherwise
        the ingest stage will fail.
        :return: The unmodified filename
        """
        return filename

    @staticmethod
    def get_non_s3_filename(filename: str):
        """
        Helper function to prevent having to check the protocol for each file name assignment when generating the
        cumulus output.
        :param filename: The current non-S3 protocols supported (http/https) require the base file name only.
        :return: The last part of the filename ie some/name/with/slashes will return slashes
        """
        return filename.rsplit('/')[-1]

    def generate_cumulus_record(self, key, value, filename_funct):
        """
        Generates a single dictionary generator that yields the expected cumulus output for a granule
        :param key: The name of the file
        :param value: A dictionary of the form {'ETag': tag, 'Last-Modified': last_mod}
        :param filename_funct: Helper function to extract the file name depending on the protocol used
        :return: A generator that will yield cumulus granule dictionaries
        """
        epoch = value.get('Last-Modified')
        host = self.provider.get('host')
        filename = filename_funct(key)
        path = key[key.find(host) + len(host): key.find(filename)]

        return {
            'granuleId': filename,
            'dataType': self.collection.get('name', ''),
            'version': self.collection.get('version', ''),
            'files': [
                {
                    'name': filename,
                    'path': path,
                    'size': '',
                    'time': epoch,
                    'bucket': self.s3_bucket_name,
                    'url_path': self.collection.get('url_path', ''),
                    'type': ''
                }
            ]
        }

    def cumulus_output_generator(self, ret_dict):
        """
        Function to generate correctly formatted output for the next step in the workflow which is queue_granules.
        :param ret_dict: Dictionary containing only newly discovered granules.
        :return: Dictionary with a list of dictionaries formatted for the queue_granules workflow step.
        """
        filename_funct = self.get_s3_filename if self.provider["protocol"] == 's3' else self.get_non_s3_filename
        return [self.generate_cumulus_record(k, v, filename_funct) for k, v in ret_dict.items()]

    def lock_db(self):
        """
        This function attempts to create a AWS S3 bucket. If the bucket already exists it will attempt to create it
        for two minutes while also calling db_lock_mitigation. Once the bucket is created it will break from the
        loop.
        """
        timeout = 120
        while timeout:
            try:
                self.db_table.put_item(
                    Item={
                        'DatabaseLocked': 'locked',
                        'LockDuration': str(time.time() + 900)
                    },
                    ConditionExpression='attribute_not_exists(DatabaseLocked)'
                )
                break
            except self.db_table.meta.client.exceptions.ConditionalCheckFailedException:
                print('waiting on lock.')
                timeout -= 1
                sleep(1)

        if not timeout:
            raise ValueError('Timeout: Unsuccessful in creating database lock.')

    def unlock_db(self):
        """
        Used to delete the "lock" bucket.
        """
        self.db_table.delete_item(
            Key={
                'DatabaseLocked': 'locked'
            }
        )

    def read_db_file(self):
        """
        Reads the SQLite database file from S3
        """
        self.lock_db()
        try:
            self.s3_client.download_file(self.s3_bucket_name, self.db_key, DB_FILE_PATH)
        except botocore.exceptions.ClientError as err:
            # The db files doesn't exist in S3 yet so create it
            db.connect()
            db.create_tables([Granule])

    def write_db_file(self):
        """
        Writes the SQLite database file to S3
        """
        db.close()
        self.s3_client.upload_file(DB_FILE_PATH, self.s3_bucket_name, self.db_key)
        self.unlock_db()

    @staticmethod
    def db_file_cleanup():
        """
        This function deletes the database file stored in the lambda as each invocation can be using a previously used
        file system with the old db file
        """
        os.remove(DB_FILE_PATH)


if __name__ == '__main__':
    pass
