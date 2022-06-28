import base64
import os
import re
import boto3
import paramiko
from task.discover_granules_base import DiscoverGranulesBase


class DiscoverGranulesSFTP(DiscoverGranulesBase):
    """
    Class to discover granules from an SFTP provider
    """
    def __init__(self, event, logger):
        super().__init__(event, logger)
        self.sftp_client = self.sftp_client()
        self.path = self.config.get('provider_path')
        self.file_reg_ex = self.collection.get('granuleIdExtraction', None)
        self.dir_reg_ex = self.discover_tf.get('dir_reg_ex', None)
        self.depth = self.discover_tf.get('depth')

    def sftp_client(self):
        port = self.provider.get('port', 22)
        transport = paramiko.Transport((self.host, port))
        username_cypher = self.provider.get('username')
        password_cypher = self.provider.get('password')
        transport.connect(None, self.decode_decrypt(username_cypher), self.decode_decrypt(password_cypher))
        return paramiko.SFTPClient.from_transport(transport)

    def decode_decrypt(self, _ciphertext):
        kms_client = boto3.client('kms')
        try:
            response = kms_client.decrypt(
                CiphertextBlob=base64.b64decode(_ciphertext),
                KeyId=os.getenv('AWS_DECRYPT_KEY_ARN')
            )
            decrypted_text = response["Plaintext"].decode()
        except Exception as err:
            self.logger.error(f'decode_decrypt exception: {err}')
            raise

        return decrypted_text

    def discover_granules(self):
        """
        Fetch the link of the granules in the host url_path
        :return: Returns a dictionary containing the path, etag, and the last modified date of a granule
        granule_dict = {
           './path/to/granule/file.extension': {
              'ETag': 'S3ETag',
              'Last-Modified': '1645564956.0
           },
           ...
        }
        """
        directory_list = []
        granule_dict = {}
        self.logger.info(f'Exploring path {self.path} depth {self.depth}')
        self.sftp_client.chdir(self.path)

        for dir_file in self.sftp_client.listdir():
            file_stat = self.sftp_client.stat(dir_file)
            file_type = str(file_stat)[0]
            if file_type == 'd' and self.check_reg_ex(self.dir_reg_ex, self.path):
                self.logger.warning(f'Found directory: {dir_file}')
                directory_list.append(dir_file)
            elif self.check_reg_ex(self.file_reg_ex, str(dir_file)):
                self.logger.warning(f'Found file: {dir_file}')
                self.populate_dict(granule_dict, f'{self.path.rstrip("/")}/{dir_file}', etag='N/A',
                                   last_mod=file_stat.st_mtime, size=file_stat.st_size)
            else:
                self.logger.warning(f'Regex did not match dir_file: {dir_file}')

        self.depth = min(abs(self.depth), 3)
        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.path = directory
                granule_dict.update(
                    self.discover_granules()
                )
        self.sftp_client.chdir('../')
        return granule_dict

    @staticmethod
    def check_reg_ex(regex, target):
        if regex is None or re.search(regex, target):
            return True
        else:
            return False


class SFTPTestFile:
    # def __new__(cls, *args, **kwargs):
        # temp = super(SFTPTestFile, cls).__new__(cls)
        # # temp.filename =
        # # temp.file_type = file_type
        # # temp.st_mtime = mod_time
        # # temp.st_size = size
        # return temp

    def __init__(self, filename, file_type, mod_time, size):
        self.filename = filename
        self.file_type = file_type
        self.st_mtime = mod_time
        self.st_size = size

    def stat(self):
        return self

    def __str__(self):
        return f'{self.file_type}'


if __name__ == "__main__":
    test_file = SFTPTestFile('test_name', 'directory', 1, 1)
    print(isinstance(test_file, str))
    print(test_file.file_type)
    print(test_file.st_size)
