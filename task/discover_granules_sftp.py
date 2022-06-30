import base64
import logging
import os
import re
import boto3
import paramiko
from paramiko import AutoAddPolicy

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from main import rdg_logger

# logging.basicConfig()
# logging.getLogger("paramiko").setLevel(logging.INFO)


def create_sftp_config(**kwargs):
    """
    Create a mapping between the cumulus provider fields and the paramiko connect(...) parameter names.
    The cumulus provider parameters can be found here:
    https://nasa.github.io/cumulus/docs/operator-docs/provider#sftp
    The paramiko connect(...) parameters can be found here:
    https://docs.paramiko.org/en/stable/api/client.html
    :return sftp_config: A dictionary with provided configuration parameters
    """
    sftp_config = {
        'hostname': kwargs.get('host', '127.0.0.1'),
        'port': kwargs.get('port', 22),
        'username': decode_decrypt(kwargs.get('username')),
        'password': decode_decrypt(kwargs.get('password')),
        'passphrase': kwargs.get('passphrase'),
        'pKey': kwargs.get('private_key'),
        'key_filename': kwargs.get('key_filename'),

    }

    for k in list(sftp_config.keys()):
        if not sftp_config[k]:
            sftp_config.pop(k)

    return sftp_config


def test_setup_ssh_sftp_client(**kwargs):
    """
    Sets up and returns a paramiko sftp client
    :return: A configured sftp client
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    ssh_client.connect(**kwargs)
    return ssh_client.open_sftp()


def decode_decrypt(_ciphertext):
    kms_client = boto3.client('kms')
    response = kms_client.decrypt(
        CiphertextBlob=base64.b64decode(_ciphertext),
        KeyId=os.getenv('AWS_DECRYPT_KEY_ARN')
    )
    decrypted_text = response["Plaintext"].decode()

    return decrypted_text


class DiscoverGranulesSFTP(DiscoverGranulesBase):
    """
    Class to discover granules from an SFTP provider
    """
    def __init__(self, event):
        super().__init__(event)
        self.path = self.config.get('provider_path')
        self.file_reg_ex = self.collection.get('granuleIdExtraction', None)
        self.dir_reg_ex = self.discover_tf.get('dir_reg_ex', None)
        self.depth = self.discover_tf.get('depth')

    def discover_granules(self):
        sftp_client = test_setup_ssh_sftp_client(**create_sftp_config(**self.provider))
        return self._discover_granules(sftp_client)

    def _discover_granules(self, sftp_client):
        directory_list = []
        granule_dict = {}
        rdg_logger.info(f'Exploring path {self.path} depth {self.depth}')
        sftp_client.chdir(self.path)

        for dir_file in sftp_client.listdir():
            file_stat = sftp_client.stat(dir_file)
            file_type = str(file_stat)[0]
            if file_type == 'd' and check_reg_ex(self.dir_reg_ex, self.path):
                rdg_logger.warning(f'Found directory: {dir_file}')
                directory_list.append(dir_file)
            elif check_reg_ex(self.file_reg_ex, str(dir_file)):
                rdg_logger.warning(f'Found file: {dir_file}')
                self.populate_dict(granule_dict, f'{self.path.rstrip("/")}/{dir_file}', etag='N/A',
                                   last_mod=file_stat.st_mtime, size=file_stat.st_size)
            else:
                rdg_logger.warning(f'Regex did not match dir_file: {dir_file}')

        self.depth = min(abs(self.depth), 3)
        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.path = directory
                granule_dict.update(
                    self._discover_granules(sftp_client)
                )
        sftp_client.chdir('../')
        return granule_dict

    # def discover_granules(self):
    #     """
    #     Fetch the link of the granules in the host url_path
    #     :return: Returns a dictionary containing the path, etag, and the last modified date of a granule
    #     granule_dict = {
    #        './path/to/granule/file.extension': {
    #           'ETag': 'S3ETag',
    #           'Last-Modified': '1645564956.0
    #        },
    #        ...
    #     }
    #     """
    #     directory_list = []
    #     granule_dict = {}
    #     rdg_logger.info(f'Exploring path {self.path} depth {self.depth}')
    #     self.sftp_client.chdir(self.path)
    # 
    #     for dir_file in self.sftp_client.listdir():
    #         file_stat = self.sftp_client.stat(dir_file)
    #         file_type = str(file_stat)[0]
    #         if file_type == 'd' and check_reg_ex(self.dir_reg_ex, self.path):
    #             rdg_logger.warning(f'Found directory: {dir_file}')
    #             directory_list.append(dir_file)
    #         elif check_reg_ex(self.file_reg_ex, str(dir_file)):
    #             rdg_logger.warning(f'Found file: {dir_file}')
    #             self.populate_dict(granule_dict, f'{self.path.rstrip("/")}/{dir_file}', etag='N/A',
    #                                last_mod=file_stat.st_mtime, size=file_stat.st_size)
    #         else:
    #             rdg_logger.warning(f'Regex did not match dir_file: {dir_file}')
    # 
    #     self.depth = min(abs(self.depth), 3)
    #     if self.depth > 0:
    #         self.depth -= 1
    #         for directory in directory_list:
    #             self.path = directory
    #             granule_dict.update(
    #                 self.discover_granules()
    #             )
    #     self.sftp_client.chdir('../')
    #     return granule_dict


if __name__ == "__main__":
    pass
