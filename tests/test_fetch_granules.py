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


if __name__ == "__main__":
    unittest.main()
