import re

import requests
import urllib3
from bs4 import BeautifulSoup
from dateutil.parser import parse

from task.dgm import SQLITE_VAR_LIMIT, safe_call, Granule
from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DiscoverGranulesHTTP(DiscoverGranulesBase):
    """
    Class to discover granules from HTTP/HTTPS provider
    """

    def __init__(self, event):
        super().__init__(event)
        self.url_path = f'{self.provider["protocol"]}://{self.host.rstrip("/")}/' \
                        f'{self.config["provider_path"].lstrip("/")}'
        self.depth = int(self.discover_tf.get('depth'))

    def discover_granules(self):
        ret_dict = {}
        session = requests.Session()
        return self.discover(session, ret_dict)

    def discover(self, session, ret_dict):
        """
        Fetch the link of the granules in the host url_path
        :return: Returns a dictionary containing the path, etag, and the last modified date of a granule
        {'http://path/to/granule/file.extension': { 'ETag': 'S3ETag', 'Last-Modified': '1645564956.0},...}
        """
        rdg_logger.info(f'Discovering in s3://{self.host}/{self.url_path}.')
        discovered_granules_count = 0
        directory_list = []
        response = session.get(self.url_path)
        html = BeautifulSoup(response.text, features='html.parser')
        for a_tag in html.findAll('a', href=True):
            url_segment = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
            path = f'{self.url_path.rstrip("/")}/{url_segment}'
            head_resp = session.head(path).headers
            etag = head_resp.get('ETag')
            last_modified = head_resp.get('Last-Modified')

            if (etag is not None or last_modified is not None) and (check_reg_ex(self.file_reg_ex, url_segment)):
                rdg_logger.info(f'Discovered granule: {path}')
                print(f'url_segment: {url_segment}')
                # The isinstance check is needed to prevent unit tests from trying to parse a MagicMock
                # object which will cause a crash during unit tests
                if isinstance(head_resp.get('Last-Modified'), str):
                    res = re.search(self.granule_id_extraction, url_segment)
                    try:
                        granule_id = res.group(1)
                        self.populate_dict(
                            ret_dict, path, etag,
                            granule_id, self.collection_id,
                            str(parse(last_modified).timestamp()), 0
                        )
                        rdg_logger.info(f'{url_segment} matched the granuleIdExtraction')
                    except AttributeError as e:
                        rdg_logger.warning(f'The collection\'s granuleIdExtraction did not match the filename.')
                        rdg_logger.warning(f'granuleIdExtraction: {self.granule_id_extraction}')
                        rdg_logger.warning(f'filename: {url_segment}')

                    if len(ret_dict) >= self.discover_tf.get('batch_size', SQLITE_VAR_LIMIT):
                        discovered_granules_count += safe_call(
                            self.db_file_path,
                            getattr(Granule, f'db_{self.duplicates}'),
                            **{"granule_dict": ret_dict, 'logger': rdg_logger}
                        )
                        ret_dict.clear()
            elif (etag is None and last_modified is None) and (check_reg_ex(self.dir_reg_ex, path)):
                directory_list.append(f'{path}/')
            else:
                rdg_logger.warning(f'Notice: {path} not processed as granule or directory. '
                                   f'The supplied regex [{self.file_reg_ex}] may not match.')

        # Make 3 as the maximum depth
        self.depth = min(abs(self.depth), 3)
        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.url_path = directory
                discovered_granules_count += self.discover(session, ret_dict)

        if len(ret_dict) > 0:
            discovered_granules_count += safe_call(
                self.db_file_path, getattr(Granule, f'db_{self.duplicates}'),
                **{"granule_dict": ret_dict, 'logger': rdg_logger}
            )
            ret_dict.clear()

        return discovered_granules_count


if __name__ == '__main__':
    pass
