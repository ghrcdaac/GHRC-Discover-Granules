import logging
import os
import boto3

from bs4 import BeautifulSoup
import requests
import re
from os import path
import botocore.exceptions


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
        self.s3_key = f"{os.getenv('s3_key_prefix').rstrip('/')}/{self.csv_file_name}"
        self.s3_bucket_name = os.getenv("bucket_name")

    @staticmethod
    def html_request(url_path: str):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = requests.get(url_path)
        return BeautifulSoup(opened_url.text, features="html.parser")

    def upload_to_s3(self, granule_dict: dict):
        """
        Upload a file to an S3 bucket
        :param granule_dict: List of granules to be written to S3
        """
        temp_str = ""
        for key, value in granule_dict.items():
            temp_str += f"{str(key)},{value['filename']},{value['date_modified']},{value['time_modified']}," \
                        f"{value['meridiem_modified']},\n"
        temp_str = temp_str[:-2]

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

            lines = response['Body'].read().decode('utf-8').split()
            for row in lines:
                values = str(row).split(',')
                granule_dict[values[0]] = {}
                granule_dict[values[0]]['filename'] = values[1]
                granule_dict[values[0]]['date_modified'] = values[2]
                granule_dict[values[0]]["time_modified"] = values[3]
                granule_dict[values[0]]["meridiem_modified"] = values[4]

        except botocore.exceptions.ClientError as nk:
            logging.error(nk)
            return {}

        return granule_dict

    def check_granule_updates(self, granule_dict: {}):
        """
        Checks stored granules and updates date, time, and meridiem values
        :param granule_dict: Dictionary of granules to check
        :return Dictionary of granules that were new or updated
        """
        new_or_updated_granules = {}
        s3_granule_dict = self.download_from_s3()
        for key, value in granule_dict.items():
            is_new_or_modified = False
            if key in s3_granule_dict:
                if s3_granule_dict[key]['date_modified'] != granule_dict[key]['date_modified']:
                    s3_granule_dict[key]['date_modified'] = value['date_modified']
                    is_new_or_modified = True
                if s3_granule_dict[key]['time_modified'] != granule_dict[key]['time_modified']:
                    s3_granule_dict[key]['time_modified'] = value['time_modified']
                    is_new_or_modified = True
                if s3_granule_dict[key]['meridiem_modified'] != granule_dict[key]['meridiem_modified']:
                    s3_granule_dict[key]['meridiem_modified'] = granule_dict[key]['meridiem_modified']
                    is_new_or_modified = True
            else:
                s3_granule_dict[key] = {}
                s3_granule_dict[key]['filename'] = value['filename']
                s3_granule_dict[key]['date_modified'] = value['date_modified']
                s3_granule_dict[key]['time_modified'] = value['time_modified']
                s3_granule_dict[key]['meridiem_modified'] = value['meridiem_modified']
                is_new_or_modified = True

            if is_new_or_modified:
                new_or_updated_granules[key] = {}
                new_or_updated_granules[key]['filename'] = value['filename']
                new_or_updated_granules[key]['date_modified'] = value['date_modified']
                new_or_updated_granules[key]['time_modified'] = value['time_modified']
                new_or_updated_granules[key]['meridiem_modified'] = value['meridiem_modified']

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
            if url_path and url_path[-1] != '/':
                url_path = f'{url_path}/'
            pre_tag = str(self.html_request(url_path).find('pre'))
            file_links = []
            file_names = []
            discovered_directories = []

            # Get all of the date, time, and meridiem data associated with each file
            date_modified_list = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", pre_tag)
            time_modified_list = re.findall(r"\d{1,2}:\d{2}", pre_tag)
            meridiem_list = re.findall("AM|PM", pre_tag, re.IGNORECASE)
            # Get the current directory/file name
            paths = re.findall("\".*?\"", pre_tag)[1:]
            paths[:] = [path.basename(p.strip('\"').rstrip("/")) for p in paths]

            # Get all of the file names, links, date modified, time modified, and meridiem.
            for file_path, date, time, meridiem in zip(paths, date_modified_list, time_modified_list, meridiem_list):
                current_path = path.basename(file_path.strip('\"').rstrip("/"))
                full_path = f'{url_path}{current_path}'

                if current_path.rfind('.') != -1:
                    if file_reg_ex is None or re.match(file_reg_ex, current_path) is not None:
                        granule_dict[full_path] = {}
                        granule_dict[full_path]['filename'] = current_path
                        granule_dict[full_path]['date_modified'] = date
                        granule_dict[full_path]['time_modified'] = time
                        granule_dict[full_path]['meridiem_modified'] = meridiem

                        file_links.append(full_path)
                        file_names.append(path.basename(current_path))
                elif depth > 0:
                    directory_path = f'{full_path}/'
                    if not dir_reg_ex or re.match(dir_reg_ex, directory_path):
                        discovered_directories.append(directory_path)

            for file_link, filename, date, time, meridiem in zip(file_links, file_names, date_modified_list,
                                                                 time_modified_list, meridiem_list):
                print(f'[link, name, date, time, meridiem] = [{file_link}, {filename}, {date}, {time}, {meridiem}]')

            depth = min(abs(depth), 3)
            if depth > 0:
                for directory in discovered_directories:
                    granule_dict.update(
                        self.get_file_links_http(url_path=directory, file_reg_ex=file_reg_ex,
                                                 dir_reg_ex=dir_reg_ex, depth=(depth - 1))
                    )

            granule_dict = self.check_granule_updates(granule_dict)
        except ValueError as ve:
            logging.error(ve)

        return granule_dict
