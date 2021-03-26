# Test here
import sys
import os
from task.main import DiscoverGranules
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
import unittest

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../task')
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestWrapperDiscoverGranules(DiscoverGranules):
    def __init__(self):
        pass


class TestDiscoverGranules(unittest.TestCase):
    _test_html = ''

    def setUp(self):
        print('tear up')
        test_file_path = os.path.join(THIS_DIR, 'test_page.html')
        with open(test_file_path, 'r') as test_html_file:
            self._test_html = test_html_file.read()

    def test_get_file_link(self):
        dg = TestWrapperDiscoverGranules()
        dg.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        dg.upload_to_s3 = MagicMock()
        dg.download_from_s3 = MagicMock()
        retrieved_dict = dg.get_file_links_http(url_path='')
        self.assertEqual(len(retrieved_dict), 5)

    def test_get_file_link_wregex(self):
        dg = TestWrapperDiscoverGranules()
        dg.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        dg.upload_to_s3 = MagicMock()
        dg.download_from_s3 = MagicMock()
        retrieved_dict = dg.get_file_links_http(url_path='', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(retrieved_dict), 1)

    def test_bad_url(self):
        dg = TestWrapperDiscoverGranules()
        retrieved_dict = dg.get_file_links_http(url_path='Bad URL', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(retrieved_dict), 0)


if __name__ == "__main__":
    unittest.main()
