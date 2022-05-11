import json
import os

from task.discover_granules_base import DiscoverGranulesBase
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
import logging
import unittest
from .helpers import get_event

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):

    def setUp(self) -> None:
        provider = {
            "host": "data.remss.com",
            "protocol": "https"

        }
        granuleIdExtraction = "^(f16_\\d{8}v7.gz)$"
        provider_path = "/ssmi/f16/bmaps_v07/y2021/"
        discover_tf = {
            "depth": 0,
            "dir_reg_ex": ".*"
        }
        event = get_event(provider, granuleIdExtraction, provider_path, discover_tf)
        self.dg = DiscoverGranulesBase(event, logging)
        self.dg.getSession = MagicMock()

    def setup_http_mock(self, name):
        """
        """
        name_html = self.get_html(name)
        name_header_responses = self.get_header_responses(name)
        self.dg.html_request = MagicMock(return_value=BeautifulSoup(name_html, features="html.parser"))
        self.dg.headers_request = MagicMock(side_effect=name_header_responses)

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

    # def test_get_file_link_remss_without_regex(self):
    #     self.setup_http_mock(name="remss")
    #     self.dg.event['config']['collection']['granuleIdExtraction'] = '^.*'
    #     retrieved_dict = self.dg.discover_granules()
    #     self.assertEqual(len(retrieved_dict), 5)
    #
    # def test_get_file_link_remss_with_regex(self):
    #     self.setup_http_mock(name="remss")
    #     self.dg.event['config']['collection']['granuleIdExtraction'] = "^(f16_\\d{8}v7.gz)$"
    #     retrieved_dict = self.dg.discover_granules()
    #     self.assertEqual(len(retrieved_dict), 3)
    #
    # def test_get_file_link_amsu_without_regex(self):
    #     self.setup_http_mock(name="msut")
    #     self.dg.event['config']['collection']['granuleIdExtraction'] = '^.*'
    #     retrieved_dict = self.dg.discover_granules()
    #     self.assertEqual(len(retrieved_dict), 4)
    #
    # def test_get_file_link_amsu_with_regex(self):
    #     self.setup_http_mock(name="msut")
    #     self.dg.event['config']['collection']['granuleIdExtraction'] = "^tlt.*\\d{4}_6\\.\\d"
    #     retrieved_dict = self.dg.discover_granules()
    #     self.assertEqual(len(retrieved_dict), 1)


if __name__ == "__main__":
    unittest.main()
