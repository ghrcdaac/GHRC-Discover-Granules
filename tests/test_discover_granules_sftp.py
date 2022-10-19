import json
import os
import re
import unittest
from unittest.mock import MagicMock, patch

import task.discover_granules_sftp as sftp

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class SFTPTestFile:
    def __init__(self, filename, file_type, mod_time, size):
        self.filename = filename
        self.file_type = file_type
        self.st_mtime = mod_time
        self.st_size = size

    def stat(self):
        return self

    def __str__(self):
        return f'{self.filename}'


class SFTPTestClient:
    def __init__(self, path, dir_count, file_count):
        self.listdir_resp = self.listdir_setup(path, dir_count, file_count)

    def listdir_setup(self, path, dir_count, file_count):
        resp = []
        for x in range(dir_count):
            resp.append(SFTPTestFile(f'dir_{x}', 'dir', 1, 1))

        for x in range(file_count):
            resp.append(SFTPTestFile(f'file_{x}', 'file', 1, 1))

        return resp

    def chdir(self, path):
        pass

    def listdir(self):
        return self.listdir_resp

    def stat(self, sftp_test_file):
        return sftp_test_file.stat()


class FakeKms:
    def __init__(self, CiphertextBlob):
        self.rsp = {'Plaintext': CiphertextBlob}

    def decrypt(self, CiphertextBlob, KeyId):
        return self.rsp


class TestDiscoverGranules(unittest.TestCase):

    def setUp(self, temp=None) -> None:
        self.dg_sftp = sftp.DiscoverGranulesSFTP(self.get_sample_event('sftp'))
    """
    will test discover granules
    """
    @staticmethod
    def get_sample_event(event_type='skip'):
        with open(os.path.join(THIS_DIR, f'input_event_{event_type}.json'), 'r', encoding='UTF-8') as test_event_file:
            return json.load(test_event_file)

    @patch('task.discover_granules_sftp.create_ssh_sftp_config')
    @patch('task.discover_granules_sftp.setup_ssh_sftp_client')
    def test_discover_granules(self, mock_sftp_setup, mock_sftp_config):
        # dg_sftp = sftp.DiscoverGranulesSFTP(self.get_sample_event('sftp'))
        self.dg_sftp.discover = MagicMock()
        self.dg_sftp.discover_granules()
        self.assertEqual(self.dg_sftp.discover.call_count, 1)
        self.assertEqual(mock_sftp_setup.call_count, 1)
        self.assertEqual(mock_sftp_config.call_count, 1)

    def test_discover_granules_sftp(self):
        event = self.get_sample_event('sftp')
        sftp_test_client = SFTPTestClient(event.get('config').get('provider_path'), 3, 3)

        res_count = self.dg_sftp.discover(sftp_test_client, {})
        expected = {
            '/ssmi/f16/bmaps_v07/y2021/m03/file_0': {
                'ETag': 'N/A', "GranuleId": "file_0", "CollectionId": "rssmif16d___7", 'Last-Modified': '1', 'Size': 1},
            '/ssmi/f16/bmaps_v07/y2021/m03/file_1': {
                'ETag': 'N/A', "GranuleId": "file_1", "CollectionId": "rssmif16d___7", 'Last-Modified': '1', 'Size': 1},
            '/ssmi/f16/bmaps_v07/y2021/m03/file_2': {
                'ETag': 'N/A', "GranuleId": "file_2", "CollectionId": "rssmif16d___7", 'Last-Modified': '1', 'Size': 1}
        }
        self.assertEqual(res_count, 3)

    @patch.object(re, 'search')
    def test_discover_granules_sftp_recursion(self, re_test):
        event = self.get_sample_event('sftp')
        event.get('config').get('collection').get('meta').get('discover_tf')['depth'] = 1
        sftp_test_client = SFTPTestClient(event.get('config').get('provider_path'), 3, 0)
        # dg_sftp = sftp.DiscoverGranulesSFTP(event)
        res_count = self.dg_sftp.discover(sftp_test_client, {})

        self.assertEqual(res_count, 0)

    @patch.object(re, 'search')
    def test_discover_granules_sftp_no_reg_ex_match(self, re_test):
        re.search = MagicMock(return_value='False')
        event = self.get_sample_event('sftp')
        event.get('config').get('collection').get('meta').get('discover_tf')['depth'] = 1
        sftp_test_client = SFTPTestClient(event.get('config').get('provider_path'), 3, 0)
        # dg_sftp = sftp.DiscoverGranulesSFTP(event)
        res_count = self.dg_sftp.discover(sftp_test_client, {})

        self.assertEqual(res_count, 0)

    @patch('paramiko.SSHClient')
    def test_setup_ssh_sftp_client(self, mock_paramiko):
        sftp_client = sftp.setup_ssh_sftp_client()
        self.assertIsNot(sftp_client, None)

    @patch('task.discover_granules_sftp.get_private_key')
    @patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    def test_create_sftp_config(self, mock_decrypt, mock_get_pkey):
        uname = 'username'
        pword = 'password'
        mock_get_pkey.side_effect = ['privateKey']
        mock_decrypt.side_effect = [uname, pword]

        config_params = {
            'host': 'host',
            'port': 22,
            'username': uname,
            'password': pword,
            'privateKey': 'privateKey',
            'key_filename': 'key_filename'
        }

        res = sftp.create_ssh_sftp_config(**config_params)
        config_values = config_params.values()
        response_values = res.values()
        for config_value, response_value in zip(config_values, response_values):
            self.assertEqual(config_value, response_value)

    @patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    def test_create_sftp_config_unset_params(self, mock_decrypt):
        uname = 'username'
        pword = 'password'
        mock_decrypt.side_effect = [uname, pword]
        config_params = {
            'host': 'host',
            'port': 22,
            'username': uname,
            'password': pword,
            'private_key': None,
            'key_filename': None
        }

        res = sftp.create_ssh_sftp_config(**config_params)
        config_values = config_params.values()
        response_values = res.values()
        for config_value, response_value in zip(config_values, response_values):
            self.assertEqual(config_value, response_value)

    def test_kms_decrypt_ciphertext(self):
        os.environ['AWS_DECRYPT_KEY_ARN'] = 'fake_arn'
        t = b'test_text'
        kms_client = FakeKms(t)
        res = sftp.kms_decrypt_ciphertext(t, kms_client)
        self.assertEqual(t.decode(), res)

    @patch('boto3.client')
    def test_kms_decrypt_ciphertext_2(self, mock_client):
        t = b'test_text'
        mock_client.side_effect = [FakeKms(t)]
        os.environ['AWS_DECRYPT_KEY_ARN'] = 'fake_arn'
        res = sftp.kms_decrypt_ciphertext(t)
        self.assertEqual(t.decode(), res)

    @patch('paramiko.rsakey.RSAKey.from_private_key')
    @patch('boto3.client')
    def test_get_private_key(self, mock_client, mock_rsa):
        temp_file = os.getcwd()
        fake_file = f'{temp_file}/fake_key'
        with open(fake_file, 'w+', encoding='utf-8') as _:
            pass
        sftp.get_private_key('fake_key', temp_file)

    @patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    def test_decrypt_credential_none(self, mock_kms):
        username = None
        mock_kms.side_effect = [username]
        sftp.decrypt_credential(username, True)
        self.assertEqual(mock_kms.call_count, 0)

    @patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    def test_decrypt_credential(self, mock_kms):
        username = 'something'
        mock_kms.side_effect = [username]
        sftp.decrypt_credential(username, True)
        self.assertEqual(mock_kms.call_count, 1)


if __name__ == "__main__":
    unittest.main()
