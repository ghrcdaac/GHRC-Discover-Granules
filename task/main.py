import logging
import timeit
from pathlib import PurePath
from typing import List

from bs4 import BeautifulSoup
import requests
import re
from os import path
import csv
import tempfile

from granule import Granule


class DiscoverGranules:
    """
    This class contains functions that fetch
    The metadata of the granules via a protocol X (HTTP/SFTP/S3)
    Compare the md5 of these granules with the ones in an S3
    It will return the files if they don't exist in S3 or the md5 doesn't match
    """
    csv_file = 'granules.csv'
    csv_path = PurePath(tempfile.gettempdir(), csv_file)

    def __init__(self):
        """
        Default values goes here
        """

    @staticmethod
    def html_request(url_path: str):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = requests.get(url_path)
        return BeautifulSoup(opened_url.text, features="html.parser")

    @staticmethod
    def check_for_updates():
        updatedList = DiscoverGranules.get_file_updates(DiscoverGranules.read_csv())
        DiscoverGranules.write_csv(updatedList)
        pass

    @staticmethod
    def write_csv(file_list):
        with open(DiscoverGranules.csv_path, 'w', newline='') as outFile:
            csvWriter = csv.writer(outFile, delimiter=',')
            for file in file_list:
                csvWriter.writerow([file.link, file.filename, file.date_modified, file.time_modified,
                                    file.meridiem_modified])

    @staticmethod
    def read_csv():
        fileList = []
        if path.exists(DiscoverGranules.csv_path):
            with open(DiscoverGranules.csv_path, 'r') as inFile:
                csvReader = csv.reader(inFile)

                # link, name, date, time, meridiem
                for row in csvReader:
                    fileList.append(Granule(str(row[0]).strip(' '), str(row[1]).strip(' '), str(row[2]).strip(' '),
                                            str(row[3]).strip(' '), str(row[4]).strip(' ')))
        return fileList

    @staticmethod
    def get_file_updates(fileList: List[Granule]):
        for i, file in enumerate(fileList):
            dir_url = file.link.rstrip(file.filename)
            pre_tag = str(DiscoverGranules.html_request(dir_url).find('pre'))

            # Get all of the date, time, and meridiem data associated with each file
            date_modified_list = re.findall("\d{1,2}/\d{1,2}/\d{4}", pre_tag)
            time_modified_list = re.findall("\d{1,2}:\d{2}", pre_tag)
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

        return fileList

    @staticmethod
    def get_files_link_http(url_path, file_reg_ex=None, dir_reg_ex=None, depth=0):
        """
        Fetch the link of the granules in the host url_path
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
            if url_path[-1] != '/':
                url_path = f'{url_path}/'
            pre_tag = str(DiscoverGranules.html_request(url_path).find('pre'))
            file_links = []
            file_names = []
            discovered_directories = []

            # Get all of the date, time, and meridiem data associated with each file
            date_modified_list = re.findall("\d{1,2}/\d{1,2}/\d{4}", pre_tag)
            time_modified_list = re.findall("\d{1,2}:\d{2}", pre_tag)
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
                        file_list.append(Granule(filename=current_path, link=full_path, date=date, time=time, meridiem=meridiem))
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
                    file_list += DiscoverGranules.get_files_link_http(url_path=directory, file_reg_ex=file_reg_ex,
                                                                      dir_reg_ex=dir_reg_ex, depth=(depth - 1))
            DiscoverGranules.write_csv(file_list)
        except ValueError as ve:
            logging.error(ve)

        return file_list


if __name__ == "__main__":
    #THis is a test
    # with syntax errors
    pass
    # Update test
    # d = DiscoverGranules
    # d.check_for_updates()
    # End test

    # URL Test without RegEx
    # d = DiscoverGranules()
    # print(f"{'==' * 6} Without regex {'==' * 6}")
    # dir_reg_ex = ".*\/y2020\/.*"
    # links = d.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m09/', depth=2)
    # for link in links:
    #     print(link)
    # End test

    # Test with file RegEx and directory RegEx
    # print(f"{'==' * 6} With regex {'==' * 6}")
    # d.links = []
    # links = d.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/',
    #                               dir_reg_ex=".*/y2011/.*", depth=3)
    # print(f' Regex list count = {len(links)}')
    #
    # for link in links:
    #     print(link)
    # End test


    # Test with file RegEx and directory RegEx
    # print(f"Found {len(links)}")
    # print(f"{'==' * 6} With regex {'==' * 6}")
    # d.links = []
    # links = d.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/',
    #                               dir_reg_ex=".*\/y2020\/.*", depth=3, file_reg_ex="^f16_\\d{4}0801v7\\.gz$")
    # print(f' Regex list count = {len(links)}')
    #
    # for link in links:
    #     print(link)
    # End test

    # Timeit Testing
#     setup = '''
# from task.main import DiscoverGranules
# from File import File
#     '''
#     call_a = '''
# temp = DiscoverGranules.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/', dir_reg_ex=".*\/y2020\/.*", depth=3, file_reg_ex="^f16_\\d{4}0801v7\\.gz$")
# print(f'Number of links found: {len(temp)}')
#     '''
#     iterations = 1
#     print(f'{timeit.timeit(setup=setup, stmt=call_a, number=iterations)/iterations}')
    # End test