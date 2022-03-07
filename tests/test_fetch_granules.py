import contextlib
import datetime
import json
import os
from dateutil.tz import tzutc
from task.main import DiscoverGranules
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
import unittest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):

    def setUp(self) -> None:
        self.dg = DiscoverGranules(self.get_sample_event())

    def tearDown(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.dg.db_file_path)
            os.remove(self.dg.db_file_path)
            os.remove(self.dg.db_file_path)

    @staticmethod
    def get_html(provider):
        with open(os.path.join(THIS_DIR, f'test_page_{provider}.html'), 'r') as test_html_file:
            return test_html_file.read()

    @staticmethod
    def get_header_responses(provider):
        with open(os.path.join(THIS_DIR, f'head_responses_{provider}.json'), 'r') as test_file:
            return json.load(test_file)['head_responses']

    @staticmethod
    def get_sample_event(event_type='skip'):
        with open(os.path.join(THIS_DIR, f'input_event_{event_type}.json'), 'r') as test_event_file:
            return json.load(test_event_file)

    def test_get_file_link_remss(self):
        self.dg.getSession = MagicMock()
        msut_html = self.get_html('remss')
        msut_header_responses = self.get_header_responses('remss')
        self.dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        self.dg.headers_request = MagicMock(side_effect=msut_header_responses)
        retrieved_dict = self.dg.discover_granules_http(url_path='fake_url')
        self.assertEqual(len(retrieved_dict), 5)

    def test_get_file_link_wregex(self):
        self.dg.getSession = MagicMock()
        msut_html = self.get_html('remss')
        msut_header_responses = self.get_header_responses('remss')
        self.dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        self.dg.headers_request = MagicMock(side_effects=msut_header_responses)
        retrieved_dict = self.dg.discover_granules_http(url_path='fake_url', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(retrieved_dict), 1)

    def test_get_file_link_msut(self):
        self.dg.getSession = MagicMock()
        msut_html = self.get_html('msut')
        msut_header_responses = self.get_header_responses('msut')
        self.dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        self.dg.headers_request = MagicMock(side_effect=msut_header_responses)
        retrieved_dict = self.dg.discover_granules_http(url_path="fake_url")
        self.assertEqual(len(retrieved_dict), 4)

    def test_get_file_link_msut_wregex(self):
        self.dg.getSession = MagicMock()
        msut_html = self.get_html('msut')
        msut_header_responses = self.get_header_responses('msut')
        self.dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        self.dg.headers_request = MagicMock(side_effect=msut_header_responses)
        retrieved_dict = self.dg.discover_granules_http(url_path="fake_url", file_reg_ex="^tlt.*\\d{4}_6.\\d+")
        self.assertEqual(len(retrieved_dict), 1)

    def test_bad_url(self):
        self.dg.html_request = MagicMock(return_value=BeautifulSoup("", features="html.parser"))
        self.dg.headers_request = MagicMock(return_value={})
        retrieved_dict = self.dg.discover_granules_http(url_path='Bad URL', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(retrieved_dict), 0)

    def test_discover_granules_s3(self):
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
        ret_dict = self.dg.discover_granules_s3('test_host', '', file_reg_ex=None, dir_reg_ex=None)
        self.assertEqual(len(ret_dict), 2)

    def test_discover_granules_s3_file_regex(self):
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
        ret_dict = self.dg.discover_granules_s3('test_host', '', file_reg_ex=".*(1.txt)", dir_reg_ex=None)
        self.assertEqual(len(ret_dict), 1)

    def test_discover_granules_s3_dir_regex(self):
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
        self.dg = DiscoverGranules(self.get_sample_event('skip_s3'))
        self.dg.get_s3_resp_iterator = MagicMock(return_value=test_resp_iter)
        ret_dict = self.dg.discover_granules_s3('test_host', '', file_reg_ex=None, dir_reg_ex=".*key1.*")
        print(ret_dict)
        self.assertEqual(len(ret_dict), 1)


if __name__ == "__main__":
    unittest.main()
