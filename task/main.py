import timeit

from bs4 import BeautifulSoup
import requests
import re
from os import path
import pathlib
import logging


from File import File


class DiscoverGranules:
    """
    This class contains functions that fetch
    The metadata of the granules via a protocol X (HTTP/SFTP/S3)
    Compare the md5 of these granules with the ones in an S3
    It will return the files if they don't exist in S3 or the md5 doesn't match
    """

    def __init__(self):
        """
        Default values goes here
        """
        self.links = []
        self.visited_links = []

    @staticmethod
    def html_request(url_path: str):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = requests.get(url_path)
        return BeautifulSoup(opened_url.text, features="html.parser")

    @staticmethod
    def get_file_info(url_path, file_regex=None, directory_regex=None, depth=0):
        file_list = []
        depth = min(abs(depth), 3)
        if depth == 0:
            return file_list

        pre_tag = str(DiscoverGranules.html_request(url_path).find('pre'))
        file_links = []
        file_names = []
        discovered_directories = []

        # Get all of the date, time, and meridiem data associated with each file
        date_modified_list = re.findall("\d{1,2}/\d{1,2}/\d{4}", pre_tag)
        time_modified_list = re.findall("\d{1,2}:\d{2}", pre_tag)
        meridiem_list = re.findall("AM|PM", pre_tag, re.IGNORECASE)
        paths = re.findall("\".*?\"", pre_tag)[1:]

        # Get all of the file names and links
        for file_path, date, time, meridiem in zip(paths, date_modified_list, time_modified_list, meridiem_list):
            current_path = path.basename(file_path.strip('\"').rstrip("/"))
            full_path = f'{url_path}{current_path}'

            if current_path.rfind('.') != -1:
                if file_regex is None or re.match(file_regex, current_path) is not None:
                    file_list.append(File(filename=current_path, link=full_path, date=date, time=time, meridiem=meridiem))
                    file_links.append(full_path)
                    file_names.append(path.basename(current_path))
            else:
                directory_path = f'{full_path}/'
                if not directory_regex or re.match(directory_regex, directory_path):
                    discovered_directories.append(directory_path)

        for file_link, filename, date, time, meridiem in zip(file_links, file_names, date_modified_list,
                                                             time_modified_list, meridiem_list):
            # if not file_regex or re.match(file_regex, filename):
            #     File(filename=filename, link=file_link, date=date, time=time, meridiem=meridiem)
            print(f'[link, name, date, time, meridiem] = [{file_link}, {filename}, {date}, {time}, {meridiem}]')

        for directory in discovered_directories:
            file_list += DiscoverGranules.get_file_info(url_path=directory, file_regex=file_regex,
                                                        directory_regex=directory_regex, depth=(depth - 1))

        return file_list

    def get_files_link_http(self, url_path: str, file_reg_ex: str = '.*', dir_reg_ex: str = '.*', depth: int = 0):
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
        depth = min(abs(depth), 3)
        url_path = f"{url_path.rstrip('/')}/"
        print(f'============> url_path: {url_path} {self.visited_links}')
        lib_path = pathlib.Path(url_path)
        parent_dir = lib_path.parent.name

        if depth < 0:
            return self.links
        try:
            # page_html = self.html_request(url_path)
            # url_path = 'http://data.remss.com/ssmi/f16/bmaps_v07', dir_reg_ex = ".*\/y2020\/.*", depth = 4, file_reg_ex = "^f16_202009.*gz$
            self.get_file_info(url_path, file_regex='f16.20200101v7.gz', depth=1)
            self.get_file_info(url_path, directory_regex='m01', depth=1)
            # self.get_file_info(page_html, url_path)

            # for index, tag in enumerate(page_html.find_all('z')):
            #     print(index)
            #     href = tag.get('href')
            #     file_name = path.basename(href)
            #     dir_name = path.basename(path.dirname(href)) if not file_name else False
            #
            #     # Only process process non-empty tags that are children to the current url_path
            #     if file_name and re.match(file_reg_ex, file_name):
            #         test = File()
            #         test.filename = file_name
            #         test.link = f'{url_path}{file_name}'
            #         self.links.append(f"{url_path}{file_name}")
            #     elif dir_name and dir_name != parent_dir:
            #         discovered_path = f"{url_path}{dir_name}/"
            #         if depth and re.match(dir_reg_ex,
            #                               f'{discovered_path}') and discovered_path not in self.visited_links:
            #             self.visited_links.append(discovered_path)
            #             self.get_files_link_http(url_path=f'{discovered_path}', file_reg_ex=file_reg_ex,
            #                                      dir_reg_ex=dir_reg_ex, depth=depth - 1)

        except ValueError as ve:
            logging.error(ve)
        self.visited_links = []

        return self.links


if __name__ == "__main__":
    # d = DiscoverGranules()
    # print(f"{'==' * 6} Without regex {'==' * 6}")
    # dir_reg_ex = ".*\/y2020\/.*"
    # # links = d.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/y2020/')
    # links = d.get_file_info(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/', directory_regex=".*\/y2020\/.*",
    #                         depth=3, file_regex="^f16_202009.*gz$")
    # for link in links:
    #     print(link)

    # print(f"Found {len(links)}")
    # print(f"{'==' * 6} With regex {'==' * 6}")
    # d.links = []
    # links = d.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/',
    #                               dir_reg_ex=".*\/y2020\/.*", depth=3, file_reg_ex="^f16_\\d{4}0801v7\\.gz$")
    # print(f' Regex list count = {len(links)}')
    #
    # for link in links:
    #     print(link)

    setup = '''
from task.main import DiscoverGranules
from File import File
    '''
    call_a = '''
d = DiscoverGranules()
temp = d.get_file_info(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/', directory_regex=".*\/y2020\/.*", depth=3, file_regex="^f16_\\d{4}0801v7\\.gz$")
print(f'Number of links found: {len(temp)}')
    '''
    iterations = 10
    print(f'{timeit.timeit(setup=setup, stmt=call_a, number=iterations)/iterations}')
