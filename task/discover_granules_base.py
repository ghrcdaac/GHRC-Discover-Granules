import os
import time
from abc import ABC, abstractmethod
import re
from tempfile import mkdtemp

from task.dgm import get_db_manager
from task.logger import rdg_logger


def check_reg_ex(regex, target):
    return regex is None or re.search(regex, target) is not None


class DiscoverGranulesBase(ABC):
    """
    Base class for discovering granules
    """

    def __init__(self, event, db_type=None, database=None):
        self.event = event
        self.input = event.get('input', {})
        self.config = event.get('config', {})
        self.provider = self.config.get('provider', {})
        self.collection = self.config.get('collection', {})
        self.collection_id = f'{self.collection.get("name")}___{self.collection.get("version")}'
        self.granule_id_extraction = self.collection.get('granuleIdExtraction', {})
        self.buckets = self.config.get('buckets', {})
        self.meta = self.collection.get('meta', {})
        self.discover_tf = self.meta.get('discover_tf', {})
        cumulus_filter = self.discover_tf.get('cumulus_filter', False)

        self.host = self.provider.get('external_host', self.provider.get('host', ''))
        self.config_stack = self.config.get('stack', {})
        self.files_list = self.config.get('collection', {}).get('files', {})
        self.file_reg_ex = self.collection.get('granuleIdExtraction', None)
        self.dir_reg_ex = self.discover_tf.get('dir_reg_ex', None)

        duplicates = str(self.collection.get('duplicateHandling', 'skip')).lower()
        force_replace = self.discover_tf.get('force_replace', False)
        # TODO: This is a temporary work around to resolve the issue with updated RSS granules not being re-ingested.
        if duplicates == 'replace' and force_replace is False:
            duplicates = 'skip'

        self.discovered_files_count = self.discover_tf.get('discovered_files_count', 0)
        self.queued_files_count = self.discover_tf.get('queued_files_count', 0)
        rdg_logger.info(f'init discovered_files_count: {self.discovered_files_count}')
        rdg_logger.info(f'init queued_files_count: {self.queued_files_count}')

        db_type = db_type if db_type else self.discover_tf.get('db_type', os.getenv('db_type', 'sqlite'))
        if db_type == 'sqlite':
            db_suffix = self.meta.get('collection_type', 'static')
            db_filename = f'discover_granules_{db_suffix}.db'
            db_file_path = f'{os.getenv("efs_path", mkdtemp())}/{db_filename}'
        else:
            db_file_path = None
        transaction_size = self.discover_tf.get('transaction_size', 100000)

        kwargs = {
            'duplicate_handling': duplicates,
            'transaction_size': transaction_size,
            'database': db_file_path,
            'db_type': db_type,
            'cumulus_filter': cumulus_filter
        }
        self.dbm = get_db_manager(**kwargs)

        super().__init__()

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

    def generate_cumulus_output(self, granule_dict_list):
        """
        Generates necessary output for the ingest workflow.
        :param granule_dict_list: Dictionary of granules discovered, ETag, Last-Modified, and Size
        :return List of dictionaries that follow this schema:
        https://github.com/nasa/cumulus/blob/master/tasks/sync-granule/schemas/input.json
        """
        temp_dict = {}
        gid_regex = self.collection.get('granuleId')
        strip_str = f'{self.provider.get("protocol")}://{self.provider.get("host")}/'

        for granule in granule_dict_list:
            file_path_name = str(granule.get('name')).replace(strip_str, '').rsplit('/', 1)
            filename = file_path_name[-1]

            file_def = self.get_file_description(filename)
            file_type = file_def.get('type', '')
            bucket_type = file_def.get('bucket', '')

            if re.search(gid_regex, filename):
                granule_id = filename
            else:
                res = re.search(self.collection.get('granuleIdExtraction'), filename)
                granule_id = res.group(1)

            if granule_id not in temp_dict:
                temp_dict[granule_id] = self.generate_cumulus_granule(granule_id)

            temp_dict[granule_id].get('files').append(
                self.generate_cumulus_file(
                    filename, file_path_name[0], granule.get('size'),
                    self.get_bucket_name(bucket_type), file_type
                )
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

    @abstractmethod
    def discover_granules(self):
        """
        Abstract method to be implemented by sub-classes
        """
        raise NotImplementedError


if __name__ == '__main__':
    pass
