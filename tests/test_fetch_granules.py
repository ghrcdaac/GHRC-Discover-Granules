# Test here
import datetime
import json
import sys
import os

from dateutil.tz import tzutc

from task.main import DiscoverGranules
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
import unittest

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../task')
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):

    @staticmethod
    def get_html(provider):
        with open(os.path.join(THIS_DIR, f'test_page_{provider}.html'), 'r') as test_html_file:
            return test_html_file.read()

    @staticmethod
    def get_header_responses(provider):
        with open(os.path.join(THIS_DIR, f'head_responses_{provider}.json'), 'r') as test_file:
            return json.load(test_file)['head_responses']

    def test_get_file_link_remss(self):
        dg = DiscoverGranules()
        dg.getSession = MagicMock()
        msut_html = self.get_html('remss')
        msut_header_responses = self.get_header_responses('remss')
        dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        dg.headers_request = MagicMock(side_effect=msut_header_responses)
        retrieved_dict = dg.discover_granules_http(url_path='fake_url')
        self.assertEqual(len(retrieved_dict), 5)

    def test_get_file_link_wregex(self):
        dg = DiscoverGranules()
        dg.getSession = MagicMock()
        msut_html = self.get_html('remss')
        msut_header_responses = self.get_header_responses('remss')
        dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        dg.headers_request = MagicMock(side_effects=msut_header_responses)
        retrieved_dict = dg.discover_granules_http(url_path='fake_url', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(retrieved_dict), 1)


    def test_get_file_link_msut(self):
        dg = DiscoverGranules()
        dg.getSession = MagicMock()
        msut_html = self.get_html('msut')
        msut_header_responses = self.get_header_responses('msut')
        dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        dg.headers_request = MagicMock(side_effect=msut_header_responses)
        retrieved_dict = dg.discover_granules_http(url_path="fake_url")
        self.assertEqual(len(retrieved_dict), 4)

    def test_get_file_link_msut_wregex(self):
        dg = DiscoverGranules()
        dg.getSession = MagicMock()
        msut_html = self.get_html('msut')
        msut_header_responses = self.get_header_responses('msut')
        dg.html_request = MagicMock(return_value=BeautifulSoup(msut_html, features="html.parser"))
        dg.headers_request = MagicMock(side_effect=msut_header_responses)
        retrieved_dict = dg.discover_granules_http(url_path="fake_url", file_reg_ex="^tlt.*\\d{4}_6.\\d+")
        self.assertEqual(len(retrieved_dict), 1)

    def test_bad_url(self):
        dg = DiscoverGranules()
        dg.html_request = MagicMock(return_value=BeautifulSoup("", features="html.parser"))
        dg.headers_request = MagicMock(return_value={})
        retrieved_dict = dg.discover_granules_http(url_path='Bad URL', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(retrieved_dict), 0)

    def test_error_exception(self):
        dg = DiscoverGranules()
        discovered_granules = {"key1": "value1"}
        s3_granules = {"key1": "value1"}
        # with self.assertRaises()
        # self.assertRaises(Exception, dg.error(discovered_granules, s3_granules))
        with self.assertRaises(Exception) as context:
            dg.error(discovered_granules, s3_granules)
            self.assertTrue('A duplicate granule was found' in context.exception)

    def test_error_no_exception(self):
        dg = DiscoverGranules()
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        s3_granules = {"granule_b": {"ETag": "tag2", "Last-Modified": "modified"}}
        ret_dict = dg.error(discovered_granules, s3_granules)
        self.assertIn("granule_a", s3_granules)
        self.assertIn("granule_a", ret_dict)

    def test_skip_no_update(self):
        dg = DiscoverGranules()
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        s3_granules = discovered_granules
        ret_dict = dg.skip(discovered_granules, s3_granules)
        self.assertEqual(len(ret_dict), 0)
        pass

    def test_skip_update_etag(self):
        dg = DiscoverGranules()
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        s3_granules = {"granule_a": {"ETag": "tag1a", "Last-Modified": "modified"}}
        ret_dict = dg.skip(discovered_granules, s3_granules)
        self.assertEqual(len(ret_dict), 1)
        self.assertEqual(discovered_granules["granule_a"]["ETag"], ret_dict["granule_a"]["ETag"])
        self.assertEqual(discovered_granules["granule_a"]["Last-Modified"], ret_dict["granule_a"]["Last-Modified"])
        pass

    def test_skip_update_modified(self):
        dg = DiscoverGranules()
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        s3_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modifieda"}}
        ret_dict = dg.skip(discovered_granules, s3_granules)
        self.assertEqual(len(ret_dict), 1)
        self.assertEqual(discovered_granules["granule_a"]["ETag"], ret_dict["granule_a"]["ETag"])
        self.assertEqual(discovered_granules["granule_a"]["Last-Modified"], ret_dict["granule_a"]["Last-Modified"])
        pass

    def test_skip_new_granule(self):
        dg = DiscoverGranules()
        discovered_granules = {"granule_a": {"ETag": "tag1a", "Last-Modified": "modifieda"}}
        s3_granules = {"granule_b": {"ETag": "tag1b", "Last-Modified": "modifiedb"}}
        ret_dict = dg.skip(discovered_granules, s3_granules)
        self.assertEqual(len(ret_dict), 1)
        self.assertEqual(len(s3_granules), 2)
        self.assertIn("granule_a", ret_dict)
        pass

    def test_skip_replace(self):
        dg = DiscoverGranules()
        discovered_granules = {"granule_a": {"ETag": "tag1a", "Last-Modified": "modifieda"}}
        s3_granules = {"granule_b": {"ETag": "tag1b", "Last-Modified": "modifiedb"}}
        ret_dict = dg.replace(discovered_granules, s3_granules)
        self.assertEqual(len(ret_dict), 1)
        self.assertIn("granule_a", ret_dict)
        self.assertNotIn("granule_b", s3_granules)
        pass

    def test_check_granule_updates_error(self):
        dg = DiscoverGranules()
        dg.error = MagicMock()
        dg.download_from_s3 = MagicMock(return_value={"granule_b": {"ETag": "tag1b", "Last-Modified": "modifiedb"}})
        dg.upload_to_s3 = MagicMock()
        discovered_granules = {"granule_a": {"ETag": "tag1a", "Last-Modified": "modifieda"}}
        dg.check_granule_updates(discovered_granules, "error")
        self.assertTrue(dg.error.called)

    def test_check_granule_updates_skip(self):
        dg = DiscoverGranules()
        dg.skip = MagicMock()
        dg.download_from_s3 = MagicMock(return_value={"granule_b": {"ETag": "tag1b", "Last-Modified": "modifiedb"}})
        dg.upload_to_s3 = MagicMock()
        discovered_granules = {"granule_a": {"ETag": "tag1a", "Last-Modified": "modifieda"}}
        dg.check_granule_updates(discovered_granules, "skip")
        self.assertTrue(dg.skip.called)

    def test_check_granule_updates_replace(self):
        dg = DiscoverGranules()
        dg.replace = MagicMock()
        dg.download_from_s3 = MagicMock(return_value={"granule_b": {"ETag": "tag1b", "Last-Modified": "modifiedb"}})
        dg.upload_to_s3 = MagicMock()
        discovered_granules = {"granule_a": {"ETag": "tag1a", "Last-Modified": "modifieda"}}
        dg.check_granule_updates(discovered_granules, "replace")
        self.assertTrue(dg.replace.called)

    def test_discover_granules_s3(self):
        test_resp_iter = [
            {
                'Contents': [
                    {
                        'Key': 'key/key1',
                        'ETag': 'etag1',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc())
                    },
                    {
                        'Key': 'key/key2',
                        'ETag': 'etag2',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc())
                    }
                ]
            }
        ]
        dg = DiscoverGranules()
        dg.get_s3_resp_iterator = MagicMock(return_value=test_resp_iter)
        ret_dict = dg.discover_granules_s3('test_host', '', file_reg_ex=None, dir_reg_ex=None)
        self.assertEqual(len(ret_dict), 2)

    def test_discover_granules_s3_file_regex(self):
        test_resp_iter = [
            {
                'Contents': [
                    {
                        'Key': 'key/key1.txt',
                        'ETag': 'etag1',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc())
                    },
                    {
                        'Key': 'key/key2.txt',
                        'ETag': 'etag2',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc())
                    }
                ]
            }
        ]
        dg = DiscoverGranules()
        dg.get_s3_resp_iterator = MagicMock(return_value=test_resp_iter)
        ret_dict = dg.discover_granules_s3('test_host', '', file_reg_ex=".*(1.txt)", dir_reg_ex=None)
        self.assertEqual(len(ret_dict), 1)

    def test_discover_granules_s3_dir_regex(self):
        test_resp_iter = [
            {
                'Contents': [
                    {
                        'Key': 'key1/key1.txt',
                        'ETag': 'etag1',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc())
                    },
                    {
                        'Key': 'key2/key2.txt',
                        'ETag': 'etag2',
                        'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc())
                    }
                ]
            }
        ]
        dg = DiscoverGranules()
        dg.get_s3_resp_iterator = MagicMock(return_value=test_resp_iter)
        ret_dict = dg.discover_granules_s3('test_host', '', file_reg_ex=None, dir_reg_ex="^key1")
        self.assertEqual(len(ret_dict), 1)


if __name__ == "__main__":
    unittest.main()
