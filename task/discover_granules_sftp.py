import base64
import os
import boto3
import paramiko
from paramiko import AutoAddPolicy

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger


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
        'username': kms_decrypt_ciphertext(kwargs.get('username')),
        'password': kms_decrypt_ciphertext(kwargs.get('password')),
        'passphrase': kwargs.get('passphrase'),
        'pKey': kwargs.get('private_key'),
        'key_filename': kwargs.get('key_filename')
    }

    for k in list(sftp_config.keys()):
        if not sftp_config[k]:
            sftp_config.pop(k)

    return sftp_config


def kms_decrypt_ciphertext(_ciphertext, kms_client=None):
    if not kms_client:
        kms_client = boto3.client('kms')
    response = kms_client.decrypt(
        CiphertextBlob=base64.b64decode(_ciphertext),
        KeyId=os.getenv('AWS_DECRYPT_KEY_ARN')
    )
    decrypted_text = response["Plaintext"].decode()

    return decrypted_text


def setup_ssh_sftp_client(**kwargs):
    """
    Sets up and returns a paramiko sftp client
    :return: A configured sftp client
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    ssh_client.connect(**kwargs)
    return ssh_client.open_sftp()


class DiscoverGranulesSFTP(DiscoverGranulesBase):
    """
    Class to discover granules from an SFTP provider
    """
    def __init__(self, event):
        super().__init__(event)
        self.path = self.config.get('provider_path')
        self.depth = self.discover_tf.get('depth')

    def discover_granules(self):
        sftp_client = setup_ssh_sftp_client(**create_sftp_config(**self.provider))
        return self.discover(sftp_client)

    def discover(self, sftp_client):
        directory_list = []
        granule_dict = {}
        rdg_logger.info(f'Exploring path {self.path} depth {self.depth}')
        sftp_client.chdir(self.path)

        for dir_file in sftp_client.listdir():
            file_stat = sftp_client.stat(dir_file)
            file_type = str(file_stat)[0]
            if file_type == 'd' and check_reg_ex(self.dir_reg_ex, self.path):
                directory_list.append(dir_file)
            elif check_reg_ex(self.file_reg_ex, str(dir_file)):
                self.populate_dict(granule_dict, f'{self.path.rstrip("/")}/{dir_file}', etag='N/A',
                                   last_mod=file_stat.st_mtime, size=file_stat.st_size)
            else:
                rdg_logger.warning(f'Notice: {dir_file} not processed as granule or directory. '
                                   f'The supplied regex [{self.file_reg_ex}] may not match.')

        self.depth = min(abs(self.depth), 3)
        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.path = directory
                granule_dict.update(
                    self.discover(sftp_client)
                )
        sftp_client.chdir('../')
        return granule_dict


if __name__ == "__main__":
    pass
