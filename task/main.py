import logging
import os
from discover_granules_http import DiscoverGranulesHTTP
from discover_granules_s3 import DiscoverGranulesS3
from discover_granules_base import DiscoverGranulesBase
from cumulus_logger import CumulusLogger
import dgm
from helpers import MyLogger

rdg_logger = CumulusLogger(name='Recursive-Discover-Granules', level=logging.INFO) if os.getenv('enable_logging', 'false').lower() == 'true' else MyLogger()


class DiscoverGranules(DiscoverGranulesBase):
    """
    This class contains functions that fetch
    The metadata of the granules via a protocol X (HTTP/SFTP/S3)
    Compare the md5 of these granules with the ones in an S3
    It will return the files if they don't exist in S3 or the md5 doesn't match
    """

    def __init__(self, event, logger=rdg_logger):
        """
        Default values goes here
        """
        super().__init__(event, logger)

        db_suffix = self.meta.get('collection_type', 'static')
        db_filename = f'discover_granules_{db_suffix}.db'
        self.db_file_path = f'{os.getenv("efs_path", "/tmp")}/{db_filename}'

    def discover(self):
        """
        Helper function to kick off the entire discover process
        """
        output = {}
        granules = self.collection.get('meta', {}).get('granules', None)
        if self.input:
            # If there is input in the event then QueueGranules failed and we need to clean out the discovered granules
            names = []
            rdg_logger.warning(self.input.get('granules', {}))
            for granule in self.input.get('granules', {}):
                file = granule.get('files')[0]
                name = f'{file.get("path")}/{file.get("name")}'
                names.append(name)

            with dgm.initialize_db(self.db_file_path):
                num = dgm.Granule().delete_granules_by_names(names)

            rdg_logger.info(f'Cleaned {num} records from the database.')
            pass
        elif granules:
            # Re-ingest: Takes provided input and generates cumulus output.
            # TODO: This should be removed as it is wasteful to load the entire lambda just to generate output.
            rdg_logger.info(f'Received {len(granules)} to re-ingest.')
            granule_dict = {}
            for granule in granules:
                self.populate_dict(granule_dict, key=granule, etag=None, last_mod=None, size=None)
            output = self.cumulus_output_generator(granule_dict)
            pass
        else:
            # Discover granules
            granule_dict = self.discover_granules()
            if not granule_dict:
                rdg_logger.warning(f'Warning: Found 0 {self.collection.get("name")} granules at the provided location.')
            else:
                rdg_logger.info(f'Discovered {len(granule_dict)} {self.collection.get("name")} '
                                f'granules for update processing.')
            self.check_granule_updates_db(granule_dict)

            output = self.cumulus_output_generator(granule_dict)
            rdg_logger.info(f'Returning cumulus output for {len(output)} {self.collection.get("name")} granules.')

        rdg_logger.info(f'Discovered {len(output)} granules.')

        if os.getenv('no_return', 'false').lower() == 'true':
            rdg_logger.warning(f'no_return is set to true. No output will be returned.')
            output = []

        return {'granules': output}

    def check_granule_updates_db(self, granule_dict: {}):
        """
        Checks stored granules and updates the datetime and ETag if updated. Expected values for duplicateHandling are
        error, replace, or skip
        :param granule_dict: Dictionary of granules to check
        :return Dictionary of granules that were new or updated
        """
        duplicates = str(self.collection.get('duplicateHandling', 'skip')).lower()
        force_replace = str(self.discover_tf.get('force_replace', 'false')).lower()
        # TODO: This is a temporary work around to resolve the issue with updated RSS granules not being re-ingested.
        if duplicates == 'replace' and force_replace == 'false':
            duplicates = 'skip'

        with dgm.initialize_db(self.db_file_path):
            getattr(dgm.Granule, f'db_{duplicates}')(dgm.Granule(), granule_dict)

        rdg_logger.info(f'{len(granule_dict)} granules remain after {duplicates} update processing.')

    def discover_granules(self):
        """
        Function to be called to trigger the granule discover process once the class has been initialized with the
        correct cumulus event
        """
        protocol = self.provider["protocol"]
        switcher = {
            'http': DiscoverGranulesHTTP,
            'https': DiscoverGranulesHTTP,
            's3': DiscoverGranulesS3
        }
        return switcher.get(protocol)(self.event, self.logger).discover_granules()


if __name__ == '__main__':
    pass
