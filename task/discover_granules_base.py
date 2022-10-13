import os
import time
from abc import ABC, abstractmethod
import re
from tempfile import mkdtemp

from task.dgm import initialize_db, Granule
from task.logger import rdg_logger


def check_reg_ex(regex, target):
    return regex is None or re.search(regex, target) is not None


class DiscoverGranulesBase(ABC):
    """
    Base class for discovering granules
    """

    def __init__(self, event):
        self.event = event
        self.input = event.get('input')
        self.config = event.get('config')
        self.provider = self.config.get('provider')
        self.collection = self.config.get('collection')
        self.granule_id_extraction = self.collection.get('granuleIdExtraction')
        self.buckets = self.config.get('buckets')
        self.meta = self.collection.get('meta')
        self.discover_tf = self.meta.get('discover_tf')
        self.host = self.provider.get('host')
        self.config_stack = self.config.get('stack')
        self.files_list = self.config.get('collection').get('files')
        self.file_reg_ex = self.collection.get('granuleIdExtraction', None)
        self.dir_reg_ex = self.discover_tf.get('dir_reg_ex', None)
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

        rdg_logger.info(f'{len(granule_dict)} granules remain after {duplicates} update processing.')

    def clean_database(self):
        """
        If there is input in the event then QueueGranules failed and we need to clean out the discovered granules
        from the database.
        """
        names = []
        rdg_logger.warning(self.input.get('granules', {}))
        for granule in self.input.get('granules', {}):
            file = granule.get('files')[0]
            name = f'{file.get("path")}/{file.get("name")}'
            names.append(name)

        with initialize_db(self.db_file_path):
            num = Granule().delete_granules_by_names(names)

        rdg_logger.info(f'Cleaned {num} records from the database.')

    def generate_lambda_output(self, ret_dict):
        if self.config.get('workflow_name') == 'LZARDSBackup':
            output_lst = self.lzards_output_generator(ret_dict)
            rdg_logger.info('LZARDS output generated')
        else:
            output_lst = self.generate_cumulus_output(ret_dict)
            rdg_logger.info('Cumulus output generated')

        return output_lst

    def get_bucket_name(self, bucket_type):
        bucket_name = ''
        for _, bv in self.buckets.items():
            if bv.get('type') == bucket_type:
                bucket_name = bv.get('name')

        return bucket_name

    def get_file_description(self, filename):
        file_desc = {}
        for file_def in self.collection.get('files'):
            if re.search(file_def.get('regex'), filename):
                file_desc = file_def

        return file_desc

    def generate_cumulus_output(self, ret_dict):
        """
        Generates necessary output for the ingest workflow.
        :param ret_dict: Dictionary of granules discovered, ETag, Last-Modified, and Size
        :return List of dictionaries that follow this schema:
        https://github.com/nasa/cumulus/blob/master/tasks/sync-granule/schemas/input.json
        """
        temp_dict = {}
        gid_regex = self.collection.get('granuleId')
        strip_str = f'{self.provider.get("protocol")}://{self.provider.get("host")}/'

        for k, v in ret_dict.items():
            file_path_name = str(k).replace(strip_str, '').rsplit('/', 1)
            filename = file_path_name[-1]

            file_def = self.get_file_description(filename)
            file_type = file_def.get('type', '')
            bucket_type = file_def.get('bucket', '')

            granule_id = ''
            if re.search(gid_regex, filename):
                granule_id = filename
            else:
                res = re.search(self.collection.get('granuleIdExtraction'), filename)
                granule_id = res.group(1)

            if granule_id not in temp_dict:
                temp_dict[granule_id] = self.generate_cumulus_granule(granule_id)

            temp_dict[granule_id].get('files').append(
                self.generate_cumulus_file(filename, file_path_name[0], v.get('Size'),
                                           self.get_bucket_name(bucket_type), file_type)
            )

        return list(temp_dict.values())

    def generate_cumulus_granule(self, granule_id):
        return {
            'granuleId': granule_id,
            'dataType': self.collection.get('name', ''),
            'version': self.collection.get('version', ''),
            'files': []
        }

    def generate_cumulus_file(self, filename, path, size, bucket_name, file_type):
        return {
            'name': filename,
            'path': path,
            'size': size,
            'time': round(time.time() * 1000),
            'url_path': self.collection.get('url_path'),
            'bucket': bucket_name,
            'type': file_type,
        }

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
    def populate_dict(target_dict, key, etag, granule_id, last_mod, size):
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
            'GranuleId': granule_id,
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
            'GranuleId': dict2.get(key).get('GranuleId'),
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
