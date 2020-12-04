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
    def get_files_link_http(url_path: str, reg_ex: str = '.*'):
        """
        Fetch the link of the granules in the host url_path
        :param url_path: The base URL where the files are served
        :type url_path: string
        :param reg_ex: Regular expression to match the files to be added
        :type reg_ex: string
        :return: links of files matching reg_ex (if reg_ex is defined)
        :rtype: list of urls
        """
        try:
            for a_href in DiscoverGranules.html_request(url_path).find_all('a'):
                file_name = path.basename(a_href.get('href'))
                if all([reg_ex, re.match(reg_ex, file_name), file_name != '', not file_name.startswith('#')]):
                    yield f"{url_path.rstrip('/')}/{file_name}"
        except ValueError as ve:
            logging.error(ve)


if __name__ == "__main__":
    d = DiscoverGranules()
    print(f"{'==' * 6} Without regex {'==' * 6}")
    links = d.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/')
    for link in links:
        print(link)
    print(f"{'==' * 6} With regex {'==' * 6}")
    links = d.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/', reg_ex="^f16_\\d{6}11v7\\.gz$")
    for link in links:
        print(link)

