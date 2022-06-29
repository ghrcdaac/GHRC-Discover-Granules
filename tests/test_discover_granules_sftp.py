import json
import logging
import os
import re
import unittest
from unittest.mock import MagicMock, patch

from task.discover_granules_sftp import DiscoverGranulesSFTP

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


class TestDiscoverGranules(unittest.TestCase):
    """
    will test discover granules
    """
    @staticmethod
    def get_sample_event(event_type='skip'):
        with open(os.path.join(THIS_DIR, f'input_event_{event_type}.json'), 'r', encoding='UTF-8') as test_event_file:
            return json.load(test_event_file)

    @patch.object(re, 'search')
    def test_discover_granules_sftp(self, re_test):
        event = self.get_sample_event('sftp')
        DiscoverGranulesSFTP.setup_sftp_client = MagicMock(
            return_value=SFTPTestClient(event.get('config').get('provider_path'), 3, 3)
        )
        dg_sftp = DiscoverGranulesSFTP(self.get_sample_event('sftp'), logging.getLogger())
        res = dg_sftp.discover_granules()
        expected = {
            '/ssmi/f16/bmaps_v07/y2021/m03/file_0': {'ETag': 'N/A', 'Last-Modified': '1', 'Size': 1},
            '/ssmi/f16/bmaps_v07/y2021/m03/file_1': {'ETag': 'N/A', 'Last-Modified': '1', 'Size': 1},
            '/ssmi/f16/bmaps_v07/y2021/m03/file_2': {'ETag': 'N/A', 'Last-Modified': '1', 'Size': 1}
        }

        self.assertEqual(res, expected)

    @patch.object(re, 'search')
    def test_discover_granules_sftp_recursion(self, re_test):
        event = self.get_sample_event('sftp')
        event.get('config').get('collection').get('meta').get('discover_tf')['depth'] = 1
        DiscoverGranulesSFTP.setup_sftp_client = MagicMock(
            return_value=SFTPTestClient(event.get('config').get('provider_path'), 3, 0)
        )
        dg_sftp = DiscoverGranulesSFTP(event, logging.getLogger())
        res = dg_sftp.discover_granules()
        expected = {}

        self.assertEqual(res, expected)

    @patch.object(re, 'search')
    def test_discover_granules_sftp_no_reg_ex_match(self, re_test):
        re.search = MagicMock(return_value='False')
        event = self.get_sample_event('sftp')
        event.get('config').get('collection').get('meta').get('discover_tf')['depth'] = 1
        DiscoverGranulesSFTP.setup_sftp_client = MagicMock(
            return_value=SFTPTestClient(event.get('config').get('provider_path'), 3, 0)
        )
        dg_sftp = DiscoverGranulesSFTP(event, logging.getLogger())
        res = dg_sftp.discover_granules()
        expected = {}

        self.assertEqual(res, expected)

    def test_check_regex_true(self):
        self.assertEqual(DiscoverGranulesSFTP.check_reg_ex('.*', 'any_text'), True)
        pass

    def test_check_regex_false(self):
        self.assertEqual(DiscoverGranulesSFTP.check_reg_ex('This will not match', 'any_text'), False)
        pass

    @patch('paramiko.Transport')
    @patch('paramiko.SFTPClient.from_transport')
    def test_setup_sftp_client(self, mock_paramiko, mock_from_transport):
        event = self.get_sample_event('sftp')
        DiscoverGranulesSFTP.decode_decrypt = MagicMock(return_value='coveralls wants coverage')
        dg_sftp = DiscoverGranulesSFTP(event, logging.getLogger())
        dg_sftp.setup_sftp_client()

    @patch('boto3.client')
    def test_decode_decrypt(self, mock_boto):
        DiscoverGranulesSFTP.decode_decrypt('')


if __name__ == "__main__":
    test_file = SFTPTestFile('test_name', 'directory', 1, 1)
    logging.warning(isinstance(test_file, str))
    # unittest.main()
