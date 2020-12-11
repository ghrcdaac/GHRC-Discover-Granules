from bs4 import BeautifulSoup
import requests
import re
from os import path
import pathlib
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
            for a_href in self.html_request(url_path).find_all('a'):
                href = a_href.get('href')
                file_name = path.basename(href)
                dir_name = path.basename(path.dirname(href)) if not file_name else False 

                #print(f'Processing file[{file_name}] href:{href} dir:{dir_name}')

                # Only process process non-empty tags that are children to the current url_path
                if file_name and re.match(file_reg_ex, file_name):
                    self.links.append(f"{url_path}{file_name}")         
                elif dir_name and dir_name != parent_dir:
                    discovered_path = f"{url_path}{dir_name}/"
                    if depth and re.match(dir_reg_ex,f'{discovered_path}') and discovered_path not in self.visited_links:
                        self.visited_links.append(discovered_path)
                        self.get_files_link_http(url_path = f'{discovered_path}',file_reg_ex=file_reg_ex,dir_reg_ex=dir_reg_ex, depth= depth - 1 )
                        
        except ValueError as ve:
            logging.error(ve)
        self.visited_links = []
        
        return self.links


if __name__ == "__main__":
    d = DiscoverGranules()
    print(f"{'==' * 6} Without regex {'==' * 6}")
    dir_reg_ex=".*\/y2020\/.*"
    links = d.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/y2020',
                                  dir_reg_ex=".*\/y2020\/m12\/.*", depth=4, file_reg_ex="^f16_.*gz$")
    for link in links:
        print(link)

    print(f"Found {len(links)}")    
    print(f"{'==' * 6} With regex {'==' * 6}")
    d.links = []
    links = d.get_files_link_http(url_path='http://data.remss.com/ssmi/f16/bmaps_v07/',
                                  dir_reg_ex=".*\/y2020\/.*", depth=3, file_reg_ex="^f16_\\d{4}0801v7\\.gz$")
    print(f' Regex list count = {len(links)}')

    for link in links:
        print(link)
