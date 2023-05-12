import re
import time

import requests
import urllib3
from bs4 import BeautifulSoup
import concurrent.futures

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DiscoverGranulesHTTP(DiscoverGranulesBase):
    """
    Class to discover granules from HTTP/HTTPS provider
    """

    def __init__(self, event):
        super().__init__(event)
        self.depth = abs(int(self.discover_tf.get('depth', 3)))

    def discover_granules(self):
        try:
            session = requests.Session()
            self.discover(session)
            self.dbm.flush_dict()
            batch = self.dbm.read_batch(self.collection_id, self.provider_url, self.discover_tf.get('batch_limit'))
        finally:
            self.dbm.close_db()

        ret = {
            'discovered_files_count': self.dbm.discovered_files_count + self.discovered_files_count,
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
        rdg_logger.info(response)
        rdg_logger.info(response.text)
        html = BeautifulSoup(response.text, features='html.parser')
        urls = []
        for a_tag in html.findAll('a', href=True):
            url_segment = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
            rdg_logger.info(f'url_segment: {url_segment}')
            full_path = f'{self.provider_url.rstrip("/")}/{url_segment}'
            urls.append(full_path)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for url in urls:
                futures.append(executor.submit(session.head, url))

            for future in concurrent.futures.as_completed(futures):
                response = future.result()
                full_path = response.url
                headers = response.headers
                etag = headers.get('ETag')
                last_modified = headers.get('Last-Modified')
                size = int(headers.get('Content-Length', 0))
                granule_id = re.search(self.granule_id_extraction, full_path)
                if granule_id:
                    self.dbm.add_record(
                        name=full_path, granule_id=granule_id.group(),
                        collection_id=self.collection_id, etag=etag,
                        last_modified=str(last_modified), size=size
                    )

                elif (etag is None and last_modified is None) and re.search(self.dir_reg_ex, full_path):
                    directory_list.append(f'{full_path}/')
                    rdg_logger.info(f'Directory found: {directory_list[-1]}')
                else:
                    rdg_logger.warning(f'Notice: {full_path} not processed as granule or directory.')

        if self.depth > 0:
            self.depth -= 1
            old_path = self.provider_url
            for directory in directory_list:
                self.provider_url = directory
                self.discover(session)
            self.provider_url = old_path


if __name__ == '__main__':
    pass
