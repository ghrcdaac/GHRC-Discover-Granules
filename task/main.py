import logging
from typing import List
import boto3

from bs4 import BeautifulSoup
import requests
import re
from os import path
import botocore.exceptions

class Granule(dict):
    """
    Simple class to make a dict look like an object.
        Example
    --------
        >>> o = Granule(key = "value")
        >>> o.key
        'value'
    """
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__


class DiscoverGranules:
    """
    This class contains functions that fetch
    The metadata of the granules via a protocol X (HTTP/SFTP/S3)
    Compare the md5 of these granules with the ones in an S3
    It will return the files if they don't exist in S3 or the md5 doesn't match
    """
    csv_file_name = 'granules.csv'

    def __init__(self):
        """
        Default values goes here
        """

    def html_request(self, url_path: str):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = requests.get(url_path)
        return BeautifulSoup(opened_url.text, features="html.parser")

    def check_for_updates(self, s3_key, bucket_name):
        """
        Gets the granule data from previous runs and checks for any updates
        :param s3_key: Key to read for granules data
        :param bucket_name: S3 bucket to read from
        :return Updated list of granules
        """
        file_list = self.download_from_s3(s3_key=s3_key, bucket_name=bucket_name)
        updated_list = self.check_granule_updates(file_list)
        self.upload_to_s3(bucket_name=bucket_name, s3_key=s3_key, granule_list=updated_list)
        return updated_list

    def upload_to_s3(self, s3_key: str, bucket_name: str, granule_list: []):
        """
        Upload a file to an S3 bucket
        :param s3_key: File to upload
        :param bucket_name: Bucket to upload to
        :param granule_list: List of granules to be written to S3
        """
        csv_formatted_str = ''
        for entry in granule_list:
            csv_formatted_str += str(entry) + ','
        csv_formatted_str = csv_formatted_str[:-1]

        client = boto3.client('s3')
        client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_formatted_str)

    def download_from_s3(self, s3_key: str, bucket_name: str):
        """
        Download a file from an S3 bucket
        :param s3_key: logical s3 file name
        :param bucket_name: Bucket to upload to
        :return: List of granules
        """
        granule_list = []
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)

        try:
            obj = bucket.Object(key=s3_key)
            response = obj.get()

            lines = response['Body'].read().decode('utf-8').split()
            for row in lines:
                values = str(row).split(',')
                granule_list.append(Granule(link=values[0], filename=values[1], date=values[2],
                                            time=values[3], meridiem=values[4]))
        except botocore.exceptions.ClientError as nk:
            logging.error(nk)
            return []

        return granule_list

    def check_granule_updates(self, granule_list: List[Granule]):
        """
        Checks stored granules and updates date, time, and meridiem values
        :param granule_list: List of granules to check
        :return Updated list of granules
        """
        for i, file in enumerate(granule_list):
            print("Checking for updates: " + str(file))
            dir_url = file.link.rstrip(file.filename)
            pre_tag = str(self.html_request(dir_url).find('pre'))

            # Get all of the date, time, and meridiem data associated with each file
            date_modified_list = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", pre_tag)
            time_modified_list = re.findall(r"\d{1,2}:\d{2}", pre_tag)
            meridiem_list = re.findall("AM|PM", pre_tag, re.IGNORECASE)
            # Get the current directory/file name
            href_values = re.findall("\".*?\"", pre_tag)[1:]
            filenames = []
            for p in href_values:
                filenames.append(path.basename(p.strip('\"').rstrip("/")))

            # Compare date, time, and meridiem for change and update if needed
            index = filenames.index(file.filename)

            if file.date_modified != date_modified_list[index]:
                file.date_modified = date_modified_list[index]
            if file.time_modified != time_modified_list[index]:
                file.time_modified = time_modified_list[index]
            if file.meridiem_modified != meridiem_list[index]:
                file.meridiem_modified = meridiem_list[index]

        return granule_list

    def get_files_link_http(self, s3_key, bucket_name, url_path, file_reg_ex=None, dir_reg_ex=None, depth=0):
        """
        Fetch the link of the granules in the host url_path
        :param s3_key: Key to be written to S3
        :param bucket_name: S3 bucket to write key to
        :param url_path: The base URL where the files are served
        :type url_path: string
        :param file_reg_ex: Regular expression used to filter files
        :type file_reg_ex: string
        :param dir_reg_ex: Regular expression used to filter directories
        :param depth: The positive number of levels to search down, will use the lesser of 3 or depth
        :return: links of files matching reg_ex (if reg_ex is defined)
        :rtype: list of urls
        """
        file_list = []

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
                        file_list.append(Granule(filename=current_path, link=full_path, date=date,
                                                 time=time, meridiem=meridiem))
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
                    file_list += self.get_files_link_http(s3_key=s3_key, bucket_name=bucket_name, url_path=directory,
                                                          file_reg_ex=file_reg_ex, dir_reg_ex=dir_reg_ex,
                                                          depth=(depth - 1))
            self.upload_to_s3(s3_key=s3_key, bucket_name=bucket_name, granule_list=file_list)
        except ValueError as ve:
            logging.error(ve)

        return file_list
