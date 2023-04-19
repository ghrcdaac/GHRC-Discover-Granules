import re

import requests
import urllib3
from bs4 import BeautifulSoup

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DiscoverGranulesHTTP(DiscoverGranulesBase):
    """
    Class to discover granules from HTTP/HTTPS provider
    """

    def __init__(self, event):
        super().__init__(event)
        self.provider_url = f'{self.provider["protocol"]}://{self.host.rstrip("/")}/' \
                            f'{self.config["provider_path"].lstrip("/")}'
        self.depth = abs(int(self.discover_tf.get('depth', 3)))

    def discover_granules(self):
        session = requests.Session()
        self.discover(session)
        self.dbm.flush_dict()
        batch = self.dbm.read_batch(self.collection_id, self.provider_url, self.discover_tf.get('batch_limit'))
        self.dbm.close_db()

        ret = {
            'discovered_files_count': self.dbm.discovered_granules_count,
            'queued_files_count': self.dbm.queued_files_count,
            'batch': batch
        }

        return ret

    def discover(self, session):
        """
        Fetch the link of the granules in the host url_path
        :return: Returns a dictionary containing the path, etag, and the last modified date of a granule
        {'http://path/to/granule/file.extension': { 'ETag': 'S3ETag', 'Last-Modified': '1645564956.0},...}
        """
        rdg_logger.info(f'Discovering in {self.provider_url}')
        directory_list = []
        response = session.get(self.provider_url)
        html = BeautifulSoup(response.text, features='html.parser')
        for a_tag in html.findAll('a', href=True):
            url_segment = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
            full_path = f'{self.provider_url.rstrip("/")}/{url_segment}'
            head_resp = session.head(full_path).headers
            etag = head_resp.get('ETag')
            last_modified = head_resp.get('Last-Modified')
            size = int(head_resp.get('Content-Length', 0))

            if (etag is not None or last_modified is not None) and (check_reg_ex(self.file_reg_ex, url_segment)):
                rdg_logger.info(f'Discovered granule: {full_path}')
                # The isinstance check is needed to prevent unit tests from trying to parse a MagicMock
                # object which will cause a crash during unit tests
                if isinstance(head_resp.get('Last-Modified'), str):
                    res = re.search(self.granule_id_extraction, url_segment)
                    if res:
                        granule_id = res.group(1)
                        self.dbm.add_record({
                            full_path: {
                                'ETag': etag,
                                'GranuleId': granule_id,
                                'CollectionId': self.collection_id,
                                'Last-Modified': str(last_modified),
                                'Size': size
                            }
                        })
                        rdg_logger.info(f'{url_segment} matched the granuleIdExtraction')
                    else:
                        rdg_logger.warning(
                            f'The collection\'s granuleIdExtraction {self.granule_id_extraction}'
                            f' did not match the filename {url_segment}.'
                        )

            elif (etag is None and last_modified is None) and (check_reg_ex(self.dir_reg_ex, full_path)):
                directory_list.append(f'{full_path}/')
            else:
                rdg_logger.warning(f'Notice: {full_path} not processed as granule or directory. '
                                   f'The supplied regex [{self.file_reg_ex}] may not match.')

        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.provider_url = directory
                self.discover(session)


if __name__ == '__main__':
    pass
