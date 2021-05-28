# Test here
import json
import sys
import os
from unittest import mock

from requests import Session

from task.main import DiscoverGranules
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
import unittest

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../task')
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):
    _test_html = ''
    _head_responses = []
    _url = "http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/"

    def setUp(self):
        test_file_path = os.path.join(THIS_DIR, 'test_page.html')
        with open(test_file_path, 'r') as test_html_file:
            self._test_html = test_html_file.read()

        test_file_path = os.path.join(THIS_DIR, 'head_responses.json')
        with open(test_file_path, 'r') as test_file:
            test = json.load(test_file)
            self._head_responses = test['head_responses']
            pass

    def test_get_file_link(self):
        dg = DiscoverGranules()
        dg.getSession = MagicMock()
        dg.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        dg.headers_request = MagicMock(side_effect=self._head_responses)
        retrieved_dict = dg.get_file_links_http(url_path=self._url)
        self.assertEqual(len(retrieved_dict), 5)

    def test_get_file_link_wregex(self):
        dg = DiscoverGranules()
        dg.getSession = MagicMock()
        dg.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        dg.headers_request = MagicMock(side_effects=self._head_responses)
        dg.upload_to_s3 = MagicMock()
        dg.download_from_s3 = MagicMock()
        retrieved_dict = dg.get_file_links_http(url_path=self._url, file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(retrieved_dict), 1)

    def test_bad_url(self):
        dg = DiscoverGranules()
        dg.html_request = MagicMock(return_value=BeautifulSoup("", features="html.parser"))
        dg.headers_request = MagicMock(return_value={})
        retrieved_dict = dg.get_file_links_http(url_path='Bad URL', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
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


if __name__ == "__main__":
    unittest.main()
