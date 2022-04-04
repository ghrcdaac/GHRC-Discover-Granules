from task.discover_granules_base import DiscoverGranulesBase
import requests
from bs4 import BeautifulSoup
import re
import urllib3
from dateutil.parser import parse


class DiscoverGranulesHTTP(DiscoverGranulesBase):
    """
       Class to discover granules from HTTP/HTTPS provider
    """

    def __init__(self, event, logger):
        super().__init__(event, logger)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()
        self.url_path = f'{self.provider["protocol"]}://{self.host.rstrip("/")}/' \
                        f'{self.config["provider_path"].lstrip("/")}'
        self.depth = int(self.discover_tf.get('depth'))

    def fetch_session(self, url, verify=False):
        """
        Establishes a session for requests.
        """
        return self.session.get(url, verify=verify)

    def html_request(self):
        """
        :param url_path: The base URL where the files are served
        :return: The html of the page if the fetch is successful
        """
        opened_url = self.fetch_session(self.url_path)
        return BeautifulSoup(opened_url.text, features='html.parser')

    def headers_request(self, url_path):
        """
        Performs a head request for the given url.
        :return Results of the request
        """
        return self.session.head(url_path).headers

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

    def discover_granules(self):
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

        file_reg_ex = self.collection.get('granuleIdExtraction')
        dir_reg_ex = self.discover_tf.get('dir_reg_ex')

        granule_dict = {}

        fetched_html = self.html_request()
        directory_list = []
        for a_tag in fetched_html.findAll('a', href=True):
            url_segment = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
            path = f'{self.url_path.rstrip("/")}/{url_segment}'
            head_resp = self.headers_request(path)
            etag = head_resp.get('ETag')
            last_modified = head_resp.get('Last-Modified')

            self.logger.info('##########')
            self.logger.info(f'Exploring a_tags for path: {path}')
            self.logger.info(f'ETag: {etag}')
            self.logger.info(f'Last-Modified: {last_modified}')

            if (etag is not None or last_modified is not None) and \
                    (file_reg_ex is None or re.search(file_reg_ex, url_segment)):
                self.logger.info(f'Discovered granule: {path}')

                granule_dict[path] = {}
                granule_dict[path]['ETag'] = str(etag)
                # The isinstance check is needed to prevent unit tests from trying to parse a MagicMock
                # object which will cause a crash during unit tests
                if isinstance(head_resp.get('Last-Modified'), str):
                    granule_dict[path]['Last-Modified'] = str(parse(last_modified).timestamp())
            elif (etag is None and last_modified is None) and \
                    (dir_reg_ex is None or re.search(dir_reg_ex, path)):
                directory_list.append(f'{path}/')
            else:
                self.logger.warning(f'Notice: {path} not processed as granule or directory. '
                                    f'The supplied regex may not match.')
        pass
        # Make 3 as the maximum depth
        self.depth = min(abs(self.depth), 3)
        if self.depth > 0:
            print(directory_list)
            for directory in directory_list:

                self.url_path = directory
                granule_dict.update(
                    self.discover_granules()
                )
            self.depth -= 1
        return granule_dict
