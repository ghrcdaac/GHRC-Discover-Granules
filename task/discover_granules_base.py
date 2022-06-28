import os
from abc import ABC, abstractmethod
import re
from tempfile import mkdtemp

from task.dgm import initialize_db, Granule


class DiscoverGranulesBase(ABC):
    """
    Base class for discovering granules
    """

    def __init__(self, event, logger):
        self.logger = logger
        self.logger.warning(f'Event: {event}')
        self.event = event
        self.input = event.get('input')
        self.config = event.get('config')
        self.provider = self.config.get('provider')
        self.collection = self.config.get('collection')
        self.meta = self.collection.get('meta')
        self.discover_tf = self.meta.get('discover_tf')
        self.host = self.provider.get('host')
        self.config_stack = self.config.get('stack')
        self.files_list = self.config.get('collection').get('files')
        db_suffix = self.meta.get('collection_type', 'static')
        db_filename = f'discover_granules_{db_suffix}.db'
        self.db_file_path = f'{os.getenv("efs_path", mkdtemp())}/{db_filename}'
        super().__init__()

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

        with initialize_db(self.db_file_path):
            getattr(Granule, f'db_{duplicates}')(Granule(), granule_dict)

        self.logger.info(f'{len(granule_dict)} granules remain after {duplicates} update processing.')

    def generate_lambda_output(self, ret_dict):
        if self.config.get('workflow_name') == 'LZARDSBackup':
            output_lst = self.lzards_output_generator(ret_dict)
            self.logger.info('LZARDS output generated')
        else:
            output_lst = self.generate_cumulus_output_new(ret_dict)
            self.logger.info('Cumulus output generated')

        return output_lst

    def generate_cumulus_output_new(self, ret_dict):
        """
        Generates necessary output for the ingest workflow.
        :param ret_dict: Dictionary of granules discovered, ETag, Last-Modified, and Size
        :return List of dictionaries that follow this schema:
        https://github.com/nasa/cumulus/blob/master/tasks/sync-granule/schemas/input.json
        """
        ret_lst = []
        for k in ret_dict:
            strip_str = f'{self.provider.get("protocol")}://{self.provider.get("host")}/'
            file_path_name = str(k).replace(strip_str, '').rsplit('/', 1)
            filename = file_path_name[-1]
            ret_lst.append(
                {
                    'granuleId': filename,
                    'dataType': self.collection.get('name', ''),
                    'version': self.collection.get('version', ''),
                    'files': [
                        {
                            'name': filename,
                            'path': file_path_name[0],
                            'type': '',
                        }
                    ]
                }
            )

        return ret_lst

    def create_file_mapping(self):
        """
        Creates a mapping from the collection file list for generating LZARDS output.
        :return A dictionary of the following format:
        { '<regex>': {'bucket': <bucket_name>, 'lzards': <True/False>}, ...}
        """
        mapping = {}
        for file_dict in self.files_list:
            bucket = file_dict.get('bucket')
            reg = file_dict.get('regex')
            lzards = file_dict.get('lzards', {}).get('backup')
            mapping[reg] = {'bucket': bucket, 'lzards': lzards}

        return mapping

    def lzards_output_generator(self, ret_dict):
        """
        Generates a single dictionary generator that yields the expected cumulus output for a granule
        :param ret_dict: Dictionary containing discovered granules, ETag, Last-Modified, and Size
        :return: A list of dictionaries that follows this schema:
        https://github.com/nasa/cumulus/blob/master/tasks/lzards-backup/schemas/input.json
        """
        strip_str = f'{self.provider.get("protocol")}://{self.provider.get("host")}/'
        mapping = self.create_file_mapping()
        ret_lst = []
        for key, value in ret_dict.items():
            filename = str(key).rsplit('/', 1)[-1]
            version = self.collection.get('version', '')

            bucket = ''
            for reg_key, val in mapping.items():
                res = re.search(reg_key, filename)
                if res:
                    bucket = val.get("bucket")
                    break

            ret_lst.append(
                {
                    'granuleId': filename,
                    'dataType': self.collection.get('name', ''),
                    'version': version,
                    'files': [
                        {
                            'bucket': f'{self.config_stack}-{bucket}',
                            'checksum': value.get('ETag'),
                            'checksumType': 'md5',
                            'key': key.replace(strip_str, ''),
                            'size': value.get('Size'),
                            'source': '',
                            'type': '',
                        }
                    ]
                }
            )

        return ret_lst

    @staticmethod
    def populate_dict(target_dict, key, etag, last_mod, size):
        """
        Helper function to populate a dictionary with ETag and Last-Modified fields.
        Clarifying Note: This function works by exploiting the mutability of dictionaries
        :param target_dict: Dictionary to add a sub-dictionary to
        :param key: Value that will function as the new dictionary element key
        :param etag: The value of the ETag retrieved from the provider server
        :param last_mod: The value of the Last-Modified value retrieved from the provider server
        :param size: File size of the discovered granule
        """
        target_dict[key] = {
            'ETag': etag,
            'Last-Modified': str(last_mod),
            'Size': size
        }

    @staticmethod
    def update_etag_lm(dict1, dict2, key):
        """
        Helper function to update the Etag and Last-Modified fields when comparing two dictionaries.
        Clarifying Note: This function works by exploiting the mutability of dictionaries
        :param dict1: The dictionary to be updated
        :param dict2: The source dictionary
        :param key: The key of the entry to be updated
        """
        dict1[key] = {
            'ETag': dict2.get(key).get('ETag'),
            'Last-Modified': dict2.get(key).get('Last-Modified'),
            'Size': dict2.get(key).get('Size'),
        }

    @abstractmethod
    def discover_granules(self):
        """
        Abstract method to be implemented by sub-classes
        """
        raise NotImplementedError


if __name__ == '__main__':
    pass
