# Test here
import os
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
from task.main import DiscoverGranules
import unittest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):

    _test_html = ''

    def setUp(self):
        print('tear up')
        test_file_path = os.path.join(THIS_DIR, 'test_page.html')
        self.d = DiscoverGranules()
        with open(test_file_path, 'r') as test_html_file:
            self._test_html = test_html_file.read()

    def test_get_file_link(self):
        self.d.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        retrieved_list = self.d.get_files_link_http('')
        self.assertEqual(len(list(retrieved_list)), 5)

    def test_get_file_link_wregex(self):
        self.d.html_request = MagicMock(return_value=BeautifulSoup(self._test_html, features="html.parser"))
        retrieved_list = self.d.get_files_link_http('', "^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(list(retrieved_list)), 1)

    def test_bad_url(self):
        retrieved_list = self.d.get_files_link_http(url_path='Bad URL', file_reg_ex="^f16_\\d{6}01v7\\.gz$")
        self.assertEqual(len(list(retrieved_list)), 0)


if __name__ == "__main__":
    unittest.main()
