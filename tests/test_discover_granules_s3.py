import datetime
import json
import os

from unittest.mock import MagicMock, patch
import unittest
from dateutil.tz import tzutc
from task.discover_granules_s3 import DiscoverGranulesS3, get_ssm_value, get_s3_client, get_s3_client_with_keys


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):
    """
    Tests Discover Granules
    """

    def setUp(self) -> None:
        self.dg = DiscoverGranulesS3(self.get_sample_event('skip_s3'))

    @staticmethod
    def get_sample_event(event_type='skip'):
        with open(os.path.join(THIS_DIR, f'input_event_{event_type}.json'), 'r', encoding='UTF-8') as test_event_file:
            return json.load(test_event_file)

    def test_get_ssm(self):
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {'Parameter': {'Value': 'test_value'}}
        ret = get_ssm_value('test_name', mock_ssm)
        self.assertEqual(ret, 'test_value')

    def test_get_s3_client(self):
        test_client = get_s3_client()
        self.assertIsNot(test_client, None)

    @patch('boto3.client')
    def test_get_s3_client_with_keys(self, mock_ssm_client):
        test_client = get_s3_client_with_keys('test_key_id', 'test_secret_key')
        self.assertIsNot(test_client, None)

    def test__discover_granules_s3(self):
        self.dg.collection['granuleIdExtraction'] = None
        self.dg.discover_tf['dir_reg_ex'] = None
        test_resp_iter = [
            {
                'Contents': [
                    {
                        'Key': 'key/key1',
                        'ETag': 'etag1',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                        'Size': 1
                    },
                    {
                        'Key': 'key/key2',
                        'ETag': 'etag2',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                        'Size': 2
                    }
                ]
            }
        ]
        ret_dict = self.dg.discover(test_resp_iter)
        self.assertEqual(ret_dict, 2)

    def test_discover_granules_s3_file_regex(self):
        self.dg.file_reg_ex = 'key1.txt'
        self.dg.dir_reg_ex = None
        test_resp_iter = [
            {
                'Contents': [
                    {
                        'Key': 'key/key1.txt',
                        'ETag': 'etag1',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                        'Size': 1
                    },
                    {
                        'Key': 'key/key2.txt',
                        'ETag': 'etag2',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                        'Size': 2
                    }
                ]
            }
        ]

        ret_dict = self.dg.discover(test_resp_iter)
        self.assertEqual(ret_dict, 1)

    def test_discover_granules_s3_dir_regex(self):
        self.dg.file_reg_ex = None
        self.dg.dir_reg_ex = 'key1'
        test_resp_iter = [
            {
                'Contents': [
                    {
                        'Key': 'key1/key1.txt',
                        'ETag': 'etag1',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                        'Size': 1
                    },
                    {
                        'Key': 'key2/key2.txt',
                        'ETag': 'etag2',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                        'Size': 1
                    }
                ]
            }
        ]

        ret_dict = self.dg.discover(test_resp_iter)
        self.assertEqual(ret_dict, 1)

    @patch('task.discover_granules_s3.get_s3_client')
    @patch('task.discover_granules_s3.get_s3_client_with_keys')
    @patch('task.discover_granules_s3.get_ssm_value')
    def test_move_granule(self, mock_ssm, mock_get_client_with_keys, mock_get_client):
        os.environ['stackName'] = 'unit-test'
        os.environ['efs_path'] = 'tmp'
        t = 's3://some_provider/at/a/path/that/is/fake.txt'
        self.dg.move_granule(t)
        del os.environ['efs_path']

    @patch('os.remove')
    @patch('task.discover_granules_s3.get_s3_client')
    @patch('task.discover_granules_s3.get_s3_client_with_keys')
    @patch('task.discover_granules_s3.get_ssm_value')
    def test_move_granule_file_exception(self, mock_ssm, mock_get_client_with_keys, mock_get_client, mock_os):
        mock_os.side_effect = MagicMock(side_effect=FileNotFoundError)
        os.environ['stackName'] = 'unit-test'
        os.environ['efs_path'] = 'tmp'
        t = 's3://some_provider/at/a/path/that/is/fake.txt'
        self.dg.move_granule(t)
        self.assertRaises(FileNotFoundError)
        del os.environ['efs_path']

    def test_move_granule_wrapper(self):
        test_dict = {
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat': {
                'ETag': 'ec5273963f74811028e38a367beaf7a5', 'Last-Modified': '1645564956.0', 'Size': 4553538},
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat': {
                'ETag': '919a1ba1dfbbd417a662ab686a2ff574', 'Last-Modified': '1645564956.0', 'Size': 4706838}}
        self.dg.move_granule = MagicMock()
        self.dg.move_granule_wrapper(test_dict)
        self.assertEqual(self.dg.move_granule.call_count, 2)


if __name__ == "__main__":
    unittest.main()
