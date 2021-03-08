# Test here
import sys
import os
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
import unittest
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../task')
from task.main import DiscoverGranules

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):

    _test_html = ''

    def setUp(self):
        print('tear up')
        test_file_path = os.path.join(THIS_DIR, 'test_page.html')
        with open(test_file_path, 'r') as test_html_file:
            self._test_html = test_html_file.read()

    def test_get_file_link(self):
        dg = DiscoverGranules()
        dg.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        retrieved_list = DiscoverGranules.get_files_link_http(s3_key='', bucket_name='', url_path='')
        self.assertEqual(len(list(retrieved_list)), 5)

    def test_get_file_link_wregex(self):
        dg = DiscoverGranules()
        dg.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        retrieved_list = DiscoverGranules.get_files_link_http(s3_key='', bucket_name='', url_path='',
                                                              file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(list(retrieved_list)), 1)

    def test_bad_url(self):
        dg = DiscoverGranules()
        retrieved_list = dg.get_files_link_http(s3_key='', bucket_name='', url_path='Bad URL',
                                                file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(list(retrieved_list)), 0)


if __name__ == "__main__":
    unittest.main()
