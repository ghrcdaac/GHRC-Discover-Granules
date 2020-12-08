import time

from bs4 import BeautifulSoup
import requests
import re
from os import path
import logging


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
        # Implement me
        pass

    @staticmethod
    def html_request(url_path: str):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = requests.get(url_path)
        return BeautifulSoup(opened_url.text, features="html.parser")

    @staticmethod
    def get_files_link_http(url_path: str, file_reg_ex: str = '.*', dir_reg_ex: str = '.*', depth: int = 0):
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
        dir_links = []
        file_links = []
        print(f'url_path: {url_path}')
        try:
            for a_href in DiscoverGranules.html_request(url_path).find_all('a'):
                tag_value = a_href.get('href')
                print(f'Processing tag [{tag_value}]')

                # Only process process non-empty tags that are children to the current url_path
                if url_path.find(tag_value) == -1 and len(tag_value) > 0:
                    # The last substring preceded by a '/' will be newly discovered
                    new_base = path.split(tag_value.rstrip('/'))[-1]
                    discovered_path = f"{url_path}{new_base}"

                    # time.sleep(2)
                    if all([tag_value[-1] != '/', re.match(file_reg_ex, new_base)]):
                        file_links.append(discovered_path)
                        print(f'File found: {file_links[-1]}')
                    elif all([depth > 0, tag_value[-1] == '/', re.match(dir_reg_ex, new_base)]):
                        dir_links.append(discovered_path)
                        print(f'Directory found: {dir_links[-1]}')
                print(f'Processing tag [{tag_value}] complete.\n\n')

        except ValueError as ve:
            logging.error(ve)

        if depth > 0:
            for dir_link in dir_links:
                print(f'dir_link: {dir_link}')
                print(f"Recursing: {url_path.rstrip('/')}{dir_link}")
                file_links = file_links + DiscoverGranules.get_files_link_http(dir_link, depth=(depth - 1))

        return file_links


if __name__ == "__main__":
    d = DiscoverGranules()
    print(f"{'==' * 6} Without regex {'==' * 6}")
    # links = d.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/', depth=2)
    links = d.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/', depth=2)
    for link in links:
        print(link)
    print(f'Found {len(links)} files.')
    print(f"{'==' * 6} With regex {'==' * 6}")
    links = d.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/',
                                  file_reg_ex="^f16_\\d{6}01v7\\.gz$")
    print(f' Regex list count = {len(links)}')
    for link in links:
        print(link)

