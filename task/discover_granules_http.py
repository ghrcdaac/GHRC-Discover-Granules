import re

import dateparser
import requests
import urllib3
from bs4 import BeautifulSoup
import concurrent.futures

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import gdg_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DiscoverGranulesHTTP(DiscoverGranulesBase):
    """
    Class to discover granules from HTTP/HTTPS provider
    """

    def __init__(self, event, context):
        super().__init__(event, context=context)
        self.depth = abs(int(self.discover_tf.get('depth', 3)))

    def discover_granules(self):
        gdg_logger.info(f'granule_id_extraction": {self.granule_id_extraction}')
        gdg_logger.info(f'granule_id": {self.granule_id}')
        try:
            session = requests.Session()
            self.discover(session)
            self.dbm.flush_dict()
            batch = self.dbm.read_batch()
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
        gdg_logger.info(f'Discovering in {self.provider_url}')
        directory_list = []
        response = session.get(self.provider_url)
        html = BeautifulSoup(response.text, features='html.parser')
        urls = []
        granule_count = 0
        for a_tag in html.findAll('a', href=True):
            href = a_tag.get('href')
            if href not in self.provider_url:
                url_segment = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
                # gdg_logger.info(f'segment: {url_segment}')
                full_path = f'{self.provider_url.rstrip("/")}/{url_segment}'
                urls.append(full_path)
            else:
                # gdg_logger.info(f'ignoring parent directory: {href}')
                pass

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for url in urls:
                futures.append(executor.submit(session.head, url))

            for future in concurrent.futures.as_completed(futures):
                response = future.result()
                full_path = response.url
                headers = response.headers
                etag = headers.get('ETag', '').strip('"')
                last_modified = headers.get('Last-Modified', '')
                size = int(headers.get('Content-Length', 0))
                print(f're.search {self.granule_id_extraction} vs {full_path}')
                granule_id = re.search(self.granule_id_extraction, full_path)
                print(granule_id)
                if granule_id:
                    self.dbm.add_record(
                        name=full_path, granule_id=granule_id.group(),
                        collection_id=self.collection_id, etag=etag,
                        last_modified=dateparser.parse(last_modified), size=size
                    )
                    granule_count += 1

                elif (not etag and not last_modified) and check_reg_ex(self.dir_reg_ex, full_path):
                    directory_list.append(f'{full_path}/')
                    gdg_logger.info(f'Directory found: {directory_list[-1]}')
                else:
                    # gdg_logger.warning(f'Notice: {full_path} not processed as granule or directory.')
                    pass

        gdg_logger.info(f'Granules found: {granule_count}')
        gdg_logger.info(f'Directories found: {len(directory_list)}')

        if self.depth > 0 and len(directory_list) > 0:
            self.depth -= 1
            current_path = self.provider_url
            for directory in directory_list:
                self.provider_url = directory
                self.discover(session)
            self.provider_url = current_path
            self.depth += 1


if __name__ == '__main__':
    pass
