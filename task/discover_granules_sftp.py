import base64
import os
import re
import tempfile

import boto3

import paramiko

from task.dgm import safe_call, SQLITE_VAR_LIMIT, Granule
from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger


def get_private_key(private_key, local_dir=None):
    """
    Downloads a private key from s3 and returns a paramiko RSAKey for authenticating.
    :param private_key: The filename of the key in s3
    :param local_dir: Optional parameter to specific location to save key file
    :return pkey: Initialize paramiko RSAKey
    """
    client = boto3.client('s3')
    tmp_dir = local_dir if local_dir else tempfile.gettempdir()
    tmp_filename = f'{tmp_dir}/{private_key}'
    client.download_file(Bucket=os.getenv('system_bucket'),
                         Key=f'{os.getenv("stackName")}/crypto/{private_key}',
                         Filename=tmp_filename)

    with open(tmp_filename, 'r+', encoding='utf-8') as data:
        pkey = paramiko.rsakey.RSAKey.from_private_key(file_obj=data)
    os.remove(tmp_filename)

    return pkey


def decrypt_credential(credential, encrypted):
    """
    Handles decrypting username and password credentials
    :param credential: Expected to be the username or password
    :param encrypted: Whether or not the value is encrypted
    :return The
    """
    ret = credential
    if encrypted and credential:
        ret = kms_decrypt_ciphertext(credential)

    return ret


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
    sftp_config = {
        'hostname': kwargs.get('host', '127.0.0.1'),
        'port': kwargs.get('port', 22),
        'username': decrypt_credential(kwargs.get('username'), encrypted),
        'password': decrypt_credential(kwargs.get('password'), encrypted),
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
        ret_dict = {}
        sftp_client = setup_ssh_sftp_client(**create_ssh_sftp_config(**self.provider))
        return self.discover(sftp_client, ret_dict)

    def discover(self, sftp_client, ret_dict):
        discovered_granules_count = 0
        directory_list = []
        rdg_logger.info(f'Exploring path {self.path} depth {self.depth}')
        sftp_client.chdir(self.path)

        for dir_file in sftp_client.listdir():
            file_stat = sftp_client.stat(dir_file)
            file_type = str(file_stat)[0]
            if file_type == 'd' and check_reg_ex(self.dir_reg_ex, self.path):
                directory_list.append(dir_file)
            elif check_reg_ex(self.file_reg_ex, str(dir_file)):
                reg_match = re.match(self.granule_id_extraction, str(dir_file))
                if reg_match is not None:
                    granule_id = re.match(self.granule_id_extraction, str(dir_file)).group(1)
                else:
                    raise ValueError(f'The granuleIdExtraction {self.granule_id_extraction} '
                                     f'did not match the file name.')
                self.populate_dict(
                    ret_dict, f'{self.path.rstrip("/")}/{dir_file}', etag='N/A',
                    granule_id=granule_id, collection_id=self.collection_id,
                    last_mod=file_stat.st_mtime, size=file_stat.st_size
                )
                if len(ret_dict) >= self.discover_tf.get('batch_size', SQLITE_VAR_LIMIT):
                    discovered_granules_count += safe_call(
                        self.db_file_path,
                        getattr(Granule, f'db_{self.duplicates}'),
                        **{"granule_dict": ret_dict, 'logger': rdg_logger}
                    )
                    ret_dict.clear()
            else:
                rdg_logger.warning(f'Notice: {dir_file} not processed as granule or directory. '
                                   f'The supplied regex [{self.file_reg_ex}] may not match.')

        self.depth = min(abs(self.depth), 3)
        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.path = directory
                discovered_granules_count += self.discover(sftp_client, ret_dict)

        sftp_client.chdir('../')

        if len(ret_dict) > 0:
            discovered_granules_count += safe_call(
                self.db_file_path, getattr(Granule, f'db_{self.duplicates}'),
                **{"granule_dict": ret_dict, 'logger': rdg_logger}
            )
            ret_dict.clear()

        return discovered_granules_count


if __name__ == "__main__":
    pass
