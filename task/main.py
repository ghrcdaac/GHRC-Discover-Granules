import base64
import logging
import os

import boto3
import requests

from task.discover_granules_http import DiscoverGranulesHTTP
from task.discover_granules_s3 import DiscoverGranulesS3
from task.discover_granules_base import DiscoverGranulesBase
from cumulus_logger import CumulusLogger
from task.dgm import *
from task.discover_granules_sftp import DiscoverGranulesSFTP
from task.helpers import MyLogger

rdg_logger = CumulusLogger(name='Recursive-Discover-Granules', level=logging.INFO) \
    if os.getenv('enable_logging', 'false').lower() == 'true' else MyLogger()


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
        self.input = event.get('input')
        self.config = event.get('config')
        self.provider = self.config.get('provider')
        self.collection = self.config.get('collection')
        meta = self.collection.get('meta')
        self.discover_tf = meta.get('discover_tf')
        self.host = self.provider.get('host')

        aws_key_id = None
        aws_secret_key = None
        key_id_name = meta.get('aws_key_id_name')
        secret_key_name = meta.get('aws_secret_key_name')
        if key_id_name and secret_key_name:
            ssm_client = boto3.client('ssm')
            aws_key_id = ssm_client.get_parameter(Name=key_id_name).get('value')
            aws_secret_key = ssm_client.get_parameter(Name=secret_key_name).get('value')

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_key_id,
            aws_secret_access_key=aws_secret_key
        )
        self.session = requests.Session()

        self.config_stack = self.config.get('stack')
        self.files_list = self.config.get('collection').get('files')
        db_suffix = self.meta.get('collection_type', 'static')
        db_filename = f'discover_granules_{db_suffix}.db'
        self.db_file_path = f'{os.getenv("efs_path", "/tmp")}/{db_filename}'
        self.switcher = {
            'http': DiscoverGranulesHTTP,
            'https': DiscoverGranulesHTTP,
            's3': DiscoverGranulesS3,
            'sftp': DiscoverGranulesSFTP
        }

    @staticmethod
    def decode_decrypt(_ciphertext):
        kms_client = boto3.client('kms')
        decrypted_text = None
        try:
            response = kms_client.decrypt(
                CiphertextBlob=base64.b64decode(_ciphertext),
                KeyId=os.getenv('AWS_DECRYPT_KEY_ARN')
            )
            decrypted_text = response["Plaintext"].decode()
        except Exception as e:
            rdg_logger.error(f'decode_decrypt exception: {e}')
            raise

        return decrypted_text

    def discover(self):
        """
        Helper function to kick off the entire discover process
        """
        output = {}
        if self.input:
            # If there is input in the event then QueueGranules failed and we need to clean out the discovered granules
            # from the database.
            names = []
            rdg_logger.warning(self.input.get('granules', {}))
            for granule in self.input.get('granules', {}):
                file = granule.get('files')[0]
                name = f'{file.get("path")}/{file.get("name")}'
                names.append(name)

            with initialize_db(self.db_file_path):
                num = Granule().delete_granules_by_names(names)

            rdg_logger.info(f'Cleaned {num} records from the database.')
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

        with initialize_db(self.db_file_path):
            getattr(Granule, f'db_{duplicates}')(Granule(), granule_dict)

        rdg_logger.info(f'{len(granule_dict)} granules remain after {duplicates} update processing.')

    def discover_granules(self):
        """
        Function to be called to trigger the granule discover process once the class has been initialized with the
        correct cumulus event
        """
        protocol = self.provider["protocol"]
        self.logger.info(f'protocol: {protocol}')
        return self.switcher.get(protocol)(self.event, self.logger).discover_granules()


def populate_dict(target_dict, key, etag, last_mod, size):
    """
    Helper function to populate a dictionary with ETag and Last-Modified fields.
    Clarifying Note: This function works by exploiting the mutability of dictionaries
    :param target_dict: Dictionary to add a sub-dictionary to
    :param key: Value that will function as the new dictionary element key
    :param etag: The value of the ETag retrieved from the provider server
    :param last_mod: The value of the Last-Modified value retrieved from the provider server
    """
    target_dict[key] = {
        'ETag': etag,
        'Last-Modified': str(last_mod),
        'Size': size
    }

if __name__ == '__main__':
    pass
