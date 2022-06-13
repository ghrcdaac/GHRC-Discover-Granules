import re
import requests
import urllib3
from bs4 import BeautifulSoup
from dateutil.parser import parse
from task.discover_granules_base import DiscoverGranulesBase



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
        self.file_reg_ex = self.collection.get('granuleIdExtraction')
        self.dir_reg_ex = self.discover_tf.get('dir_reg_ex')

    def fetch_session(self, url, verify=False):
        """
        Establishes a session for requests.
        :param url: URL to establish a session at
        :param verify: If using a certification provide the path, otherwise defaults to false
        :return: session to the URL
        """
        return self.session.get(url, verify=verify)

    def html_request(self):
        """
        Fetches the http served at the url
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
        :return: Returns a dictionary containing the path, etag, and the last modified date of a granule
        {'http://path/to/granule/file.extension': { 'ETag': 'S3ETag', 'Last-Modified': '1645564956.0},...}
        """
        granule_dict = {}
        directory_list = []
        fetched_html = self.html_request()
        for a_tag in fetched_html.findAll('a', href=True):
            url_segment = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
            path = f'{self.url_path.rstrip("/")}/{url_segment}'
            head_resp = self.headers_request(path)
            etag = head_resp.get('ETag')
            last_modified = head_resp.get('Last-Modified')

            if (etag is not None or last_modified is not None) and \
                    (self.file_reg_ex is None or re.search(self.file_reg_ex, url_segment)):
                self.logger.info(f'Discovered granule: {path}')
                # The isinstance check is needed to prevent unit tests from trying to parse a MagicMock
                # object which will cause a crash during unit tests
                if isinstance(head_resp.get('Last-Modified'), str):
                    self.populate_dict(granule_dict, path, etag, str(parse(last_modified).timestamp()), 0)
            elif (etag is None and last_modified is None) and \
                    (self.dir_reg_ex is None or re.search(self.dir_reg_ex, path)):
                directory_list.append(f'{path}/')
            else:
                self.logger.warning(f'Notice: {path} not processed as granule or directory. '
                                    f'The supplied regex [{self.file_reg_ex}] may not match.')

        # Make 3 as the maximum depth
        self.depth = min(abs(self.depth), 3)
        if self.depth > 0:
            for directory in directory_list:
                self.url_path = directory
                granule_dict.update(self.discover_granules())
            self.depth -= 1

        return granule_dict
