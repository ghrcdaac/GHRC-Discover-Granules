import contextlib
import datetime
import json
import logging
import os
from dateutil.tz import tzutc

from task.discover_granules_s3 import DiscoverGranulesS3
from task.main import DiscoverGranules
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
import unittest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):

    def setUp(self) -> None:
        self.dg = DiscoverGranulesS3(self.get_sample_event('skip_s3'), logging)

    def tearDown(self) -> None:
        # with contextlib.suppress(FileNotFoundError):
        #     os.remove(self.dg.db_file_path)
        #     os.remove(self.dg.db_file_path)
        #     os.remove(self.dg.db_file_path)
        pass

    @staticmethod
    def get_sample_event(event_type='skip'):
        with open(os.path.join(THIS_DIR, f'input_event_{event_type}.json'), 'r') as test_event_file:
            return json.load(test_event_file)

    def test_discover_granules_s3(self):
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
        self.dg.get_s3_resp_iterator = MagicMock(return_value=test_resp_iter)
        ret_dict = self.dg.discover_granules()
        self.assertEqual(len(ret_dict), 2)

    def test_discover_granules_s3_file_regex(self):
        self.dg.collection['granuleIdExtraction'] = 'key1.txt'
        self.dg.discover_tf['dir_reg_ex'] = None
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
        self.dg.get_s3_resp_iterator = MagicMock(return_value=test_resp_iter)
        ret_dict = self.dg.discover_granules()
        self.assertEqual(len(ret_dict), 1)

    def test_discover_granules_s3_dir_regex(self):
        self.dg.collection['granuleIdExtraction'] = None
        self.dg.discover_tf['dir_reg_ex'] = 'key1'
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
        self.dg.get_s3_resp_iterator = MagicMock(return_value=test_resp_iter)
        ret_dict = self.dg.discover_granules()
        self.assertEqual(len(ret_dict), 1)


if __name__ == "__main__":
    unittest.main()
