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
        self.logger = logger
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

    def generate_cumulus_output_new(self, ret_dict):
        ret_lst = []
        for k, v in ret_dict.items():
            filename = str(k).rsplit('/', 1)[-1]
            ret_lst.append(
                {
                    'granuleId': filename,
                    'dataType': self.collection.get('name', ''),
                    'version': self.collection.get('version', ''),
                    'files': [
                        {
                            'name': filename,
                            'path': self.meta.get('provider_path'),
                            'type': '',
                        }
                    ]
                }
            )

        return ret_lst

    def generate_lambda_output(self, ret_dict):
        if self.meta.get('workflow_name') == 'LZARDSBackup':
            output_lst = self.lzards_output_generator(ret_dict)
        else:
            output_lst = self.generate_cumulus_output_new(ret_dict)

        return output_lst

    def lzards_output_generator(self, ret_dict):
        """
        Generates a single dictionary generator that yields the expected cumulus output for a granule
        :param key: The name of the file
        :param value: A dictionary of the form {'ETag': tag, 'Last-Modified': last_mod}
        :param mapping: Dictionary of each file extension and needed output fields from the event
        :return: A cumulus granule dictionary
        """
        mapping = {}
        for file_dict in self.files_list:
            bucket = file_dict.get('bucket')
            reg = file_dict.get('regex')
            lzards = file_dict.get('lzards', {}).get('backup')
            mapping[reg] = {'bucket': bucket, 'lzards': lzards}

        ret_lst = []
        for key, value in ret_dict.items():
            filename = str(key).rsplit('/', 1)[-1]
            version = self.collection.get('version', '')

            temp_dict = {}
            for reg_key, val in mapping.items():
                res = re.search(reg_key, filename)
                if res:
                    temp_dict.update(val)
                    break

            ret_lst.append(
                {
                    'granuleId': filename,
                    'dataType': self.collection.get('name', ''),
                    'version': version,
                    'files': [
                        {
                            'bucket': f'{self.config_stack}-{temp_dict.get("bucket")}',
                            'checksum': value.get('ETag'),
                            'checksumType': 'md5',
                            'fileName': key,
                            'size': value.get('Size'),
                            'source': '',
                            'type': '',
                        }
                    ]
                }
            )

        return ret_lst

    # def cumulus_output_generator(self, ret_dict):
    #     """
    #     Function to generate correctly formatted output for the next step in the workflow which is queue_granules.
    #     :param ret_dict: Dictionary containing only newly discovered granules.
    #     granule_dict = {
    #        'http://path/to/granule/file.extension': {
    #           'ETag': 'S3ETag',
    #           'Last-Modified': '1645564956.0
    #        },
    #        ...
    #     }
    #     :return: Dictionary with a list of dictionaries formatted for the queue_granules workflow step.
    #     """
    #     # self.logger.warning(f'File_list: {self.files_list}')
    #     # Extract the data from the files array in the event
    #     mapping = {}
    #     for file_dict in self.files_list:
    #         bucket = file_dict.get('bucket')
    #         reg = file_dict.get('regex')
    #         lzards = file_dict.get('lzards', {}).get('backup')
    #         mapping[reg] = {'bucket': bucket, 'lzards': lzards}
    #
    #     return [self.generate_cumulus_record(k, v, mapping) for k, v in ret_dict.items()]

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
