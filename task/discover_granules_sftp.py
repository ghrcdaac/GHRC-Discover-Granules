import base64
import os
import re

import boto3
import paramiko

from task.discover_granules_base import DiscoverGranulesBase


class DiscoverGranulesSFTP(DiscoverGranulesBase):
    def __init__(self, event, logger):
        super().__init__(event, logger)
        host = self.provider.get('host')
        port = self.provider.get('port', 22)

        transport = paramiko.Transport((host, port))
        username_cypher = self.provider.get('username')
        password_cypher = self.provider.get('password')
        transport.connect(None, self.decode_decrypt(username_cypher), self.decode_decrypt(password_cypher))
        self.sftp_client = paramiko.SFTPClient.from_transport(transport)
        self.path = self.config.get('provider_path')
        self.file_reg_ex = self.collection.get('granuleIdExtraction', None)
        self.dir_reg_ex = self.discover_tf.get('dir_reg_ex', None)
        self.depth = self.discover_tf.get('depth')

    def decode_decrypt(self, _ciphertext):
        kms_client = boto3.client('kms')
        decrypted_text = None
        try:
            response = kms_client.decrypt(
                CiphertextBlob=base64.b64decode(_ciphertext),
                KeyId=os.getenv('AWS_DECRYPT_KEY_ARN')
            )
            decrypted_text = response["Plaintext"].decode()
        except Exception as e:
            self.logger.error(f'decode_decrypt exception: {e}')
            raise

        return decrypted_text

    def discover_granules(self):
        """
        Discover granules on an SFTP provider
        """
        directory_list = []
        granule_dict = {}
        self.logger.info(f'Exploring path {self.path} depth {self.depth}')
        self.sftp_client.chdir(self.path)

        for dir_file in self.sftp_client.listdir():
            file_stat = self.sftp_client.stat(dir_file)
            file_type = str(file_stat)[0]
            if file_type == 'd' and (self.dir_reg_ex is None or re.search(self.dir_reg_ex, self.path)):
                self.logger.info(f'Found directory: {dir_file}')
                directory_list.append(dir_file)
            elif self.file_reg_ex is None or re.search(self.file_reg_ex, dir_file):
                self.populate_dict(granule_dict, f'{self.path}/{dir_file}', etag='N/A',
                                   last_mod=file_stat.st_mtime, size=file_stat.st_size)
            else:
                self.logger.warning(f'Regex did not match dir_file: {dir_file}')

        depth = min(abs(self.depth), 3)
        if depth > 0:
            for directory in directory_list:
                self.path = directory
                granule_dict.update(
                    self.discover_granules()
                )
        self.sftp_client.chdir('../')
        return granule_dict
