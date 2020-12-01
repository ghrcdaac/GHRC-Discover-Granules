from bs4 import BeautifulSoup
import urllib.request
import re
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
    def get_files_link_http(url_path: str, reg_ex: str = None, recursive: bool = False):
        """
        Fetch the link of the granules in the host url_path
        :param url_path: The base URL where the files are served
        :type url_path: string
        :param reg_ex: Regular expression to match the files to be added
        :type reg_ex: string
        :return: links of files matching reg_ex (if reg_ex is defined)
        :rtype: list of urls
        """

        file_paths = []
        try:
            top_level_url = url_path[0:(url_path.find('.com') + 4)]
            opened_url = urllib.request.urlopen(url_path)
            html_soup = BeautifulSoup(opened_url.read(), 'html5lib')
            a_href_resultSet = html_soup.find_all('a')

            for a_href in a_href_resultSet:
                file_path = a_href.get('href')
                if (reg_ex is None or type(re.search(reg_ex, file_path)) is re.Match) and file_path.endswith('gz'):
                    file_paths.append(f'{top_level_url}{file_path}')
        except ValueError as ve:
            logging.error(ve)

        return file_paths


