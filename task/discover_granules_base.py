import os
import time
from abc import ABC, abstractmethod
import re
from tempfile import mkdtemp

from task.dbm_get import get_db_manager
from task.logger import gdg_logger


def check_reg_ex(regex, target):
    return regex is None or re.search(regex, target) is not None


class DiscoverGranulesBase(ABC):
    """
    Base class for discovering granules
    """

    def __init__(self, event, db_type=None, context=None):
        self.bookmark = None
        self.lambda_context = context
        self.input = event.get('input', {})
        self.config = event.get('config', {})
        self.provider = self.config.get('provider', {})
        self.collection = self.config.get('collection', {})
        self.collection_id = f'{self.collection.get("name")}___{self.collection.get("version")}'
        self.granule_id = self.collection.get('granuleId')
        self.granule_id_extraction = self.collection.get('granuleIdExtraction')
        self.buckets = self.config.get('buckets', {})
        self.meta = self.collection.get('meta', {})
        self.discover_tf = self.meta.get('discover_tf', {})
        self.host = self.provider.get('external_host', self.provider.get('host', ''))
        self.config_stack = self.config.get('stack', {})
        self.files_list = self.config.get('collection', {}).get('files', {})
        self.file_reg_ex = self.collection.get('granuleIdExtraction', None)
        self.dir_reg_ex = self.discover_tf.get('dir_reg_ex', None)
        self.duplicates = str(self.collection.get('duplicateHandling', 'skip')).lower()
        self.force_replace = self.discover_tf.get('force_replace', False)
        # TODO: This is a temporary work around to resolve the issue with updated RSS granules not being re-ingested.
        if self.duplicates == 'replace' and self.force_replace is False and not self.use_cumulus_filter:
            self.duplicates = 'skip'
        self.use_cumulus_filter = self.discover_tf.get('cumulus_filter', False)
        self.discovered_files_count = self.discover_tf.get('discovered_files_count', 0)
        self.queued_files_count = self.discover_tf.get('queued_files_count', 0)

        protocol = self.provider.get('protocol', '')
        host = self.host.strip('/')
        provider_path = str(self.config.get('provider_path', '')).lstrip('/')
        if str(protocol).lower() != 's3' and not str(provider_path).endswith('/'):
            provider_path = f'{provider_path}/'

        self.provider_url = f'{protocol}://{host}/{provider_path}'
        gdg_logger.info(f'init discovered_files_count: {self.discovered_files_count}')
        gdg_logger.info(f'init queued_files_count: {self.queued_files_count}')

        db_type = db_type if db_type else self.discover_tf.get('db_type', os.getenv('db_type', 'sqlite'))
        if db_type == 'sqlite':
            db_suffix = self.meta.get('collection_type', 'static')
            db_filename = f'discover_granules_{db_suffix}.db'
            db_file_path = f'{os.getenv("efs_path", mkdtemp())}/{db_filename}'
        else:
            db_file_path = None
        self.transaction_size = self.discover_tf.get('transaction_size', 100000)

        kwargs = {
            'duplicate_handling': self.duplicates,
            'transaction_size': self.transaction_size,
            'database': db_file_path,
            'db_type': db_type,
            'batch_limit': self.discover_tf.get('batch_limit'),
            'collection_id': self.collection_id,
            'provider_url': self.provider_url
        }

        if self.use_cumulus_filter:
            cumulus_kwargs = dict(kwargs)
            cumulus_kwargs.update({'db_type': 'cumulus', 'database': None})
            cumulus_dbm = get_db_manager(**cumulus_kwargs)
            kwargs.update({
                'cumulus_filter_dbm': cumulus_dbm
            })

        self.dbm = get_db_manager(**kwargs)

        super().__init__()

    def generate_lambda_output(self, ret_dict):
        if self.config.get('workflow_name') == 'LZARDSBackup':
            output_lst = self.lzards_output_generator(ret_dict)
            gdg_logger.info('LZARDS output generated')
        else:
            output_lst = self.generate_cumulus_output(ret_dict)
            gdg_logger.info('Cumulus output generated')

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
        for granule in granule_dict_list:
            granule_name = granule.get('name')
            res = granule_name.find(self.config.get('provider_path'))
            absolute_path = granule_name[res:]
            path_and_name = absolute_path.rsplit('/', maxsplit=1)
            path = path_and_name[0]
            filename = path_and_name[1]

            file_def = self.get_file_description(filename)
            file_type = file_def.get('type', '')
            bucket_type = file_def.get('bucket', '')

            # TODO: This can be simplified to just use the granuleIdExtraction once collections use a corrected one
            gid_match = re.search(self.granule_id_extraction, filename)
            if gid_match:
                granule_id = gid_match.group()
            else:
                granule_id = re.search(self.granule_id, filename).group()

            if granule_id not in temp_dict:
                temp_dict[granule_id] = self.generate_cumulus_granule(granule_id)
            temp_dict[granule_id].get('files').append(
                self.generate_cumulus_file(
                    filename, path, granule.get('size'),
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

    def lzards_output_generator(self, granule_dict_list):
        """
        Generates a single dictionary generator that yields the expected cumulus output for a granule
        :param granule_dict_list: List of dictionaries containing discovered granules, ETag, Last-Modified, and Size
        :return: A list of dictionaries that follows this schema:
        https://github.com/nasa/cumulus/blob/master/tasks/lzards-backup/schemas/input.json
        """
        strip_str = f'{self.provider.get("protocol")}://{self.provider.get("host")}/'
        mapping = self.create_file_mapping()
        ret_lst = []
        # for key, value in ret_dict.items():
        for granule in granule_dict_list:
            name = granule.get('name')
            filename = str(name).rsplit('/', 1)[-1]
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
                    'provider': 'private_bucket',
                    'createdAt': time.time(),
                    'files': [
                        {
                            'bucket': f'{self.config_stack}-{bucket}',
                            'key': name.replace(strip_str, ''),
                            'size': granule.get('size'),
                        }
                    ]
                }
            )

        return ret_lst

    def read_batch(self):
        try:
            batch = self.dbm.read_batch()
        finally:
            self.dbm.close_db()

        ret = {
            'discovered_files_count': self.discovered_files_count,
            'queued_files_count': self.queued_files_count + self.dbm.queued_files_count,
            'batch': batch
        }

        return ret

    @abstractmethod
    def discover_granules(self):
        """
        Abstract method to be implemented by sub-classes
        """
        raise NotImplementedError


if __name__ == '__main__':
    pass
