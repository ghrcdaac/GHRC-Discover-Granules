import base64
import os
import boto3

import paramiko

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger


def get_private_key(private_key):
    """
    Downloads a private key from s3 and returns a paramiko RSAKey for authenticating.
    :param private_key: The filename of the key in s3
    :return pkey: Initialize paramiko RSAKey
    """
    client = boto3.client('s3')
    client.download_file(
        Bucket=os.getenv('system_bucket'),
        Key=f'{os.getenv("stackName")}/crypto/{private_key}',
        Filename=f'/tmp/{private_key}'
    )
    with open(f'/tmp/{private_key}', 'r+') as data:
        pkey = paramiko.rsakey.RSAKey.from_private_key(file_obj=data)
    os.remove('/tmp/test')

    return pkey


def create_ssh_sftp_config(**kwargs):
    """
    Create a mapping between the cumulus provider fields and the paramiko connect(...) parameter names.
    The cumulus provider parameters can be found here:
    https://nasa.github.io/cumulus/docs/operator-docs/provider#sftp
    The paramiko connect(...) parameters can be found here:
    https://docs.paramiko.org/en/stable/api/client.html
    :return sftp_config: A dictionary with provided configuration parameters
    """
    encrypted = kwargs.get("encrypted", False)
    username = kwargs.get('username')
    password = kwargs.get('password')
    sftp_config = {
        'hostname': kwargs.get('host', '127.0.0.1'),
        'port': kwargs.get('port', 22),
        'username': kms_decrypt_ciphertext(username) if encrypted else username,
        'password': kms_decrypt_ciphertext(password) if encrypted else password,
        'pkey': get_private_key(kwargs.get("privateKey")) if 'privateKey' in kwargs else None,
        'key_filename': kwargs.get('key_filename')
    }

    for sftp_config_keys in list(sftp_config.keys()):
        if sftp_config_keys != 'hostkey' and not sftp_config[sftp_config_keys]:
            sftp_config.pop(sftp_config_keys)

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
    Sets up and returns a paramiko ssh sftp client
    :return: A configured sftp client
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
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
        sftp_client = setup_ssh_sftp_client(**create_ssh_sftp_config(**self.provider))
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
