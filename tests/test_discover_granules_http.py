import json
import os
from unittest.mock import MagicMock, patch
import unittest

from task.discover_granules_http import DiscoverGranulesHTTP
from .helpers import configure_event

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class FakeResponse:
    def __init__(self, text):
        self.text = text


class FakeHeadResponse:
    def __init__(self, headers):
        self.headers = headers
        pass


class TestDiscoverGranules(unittest.TestCase):
    """
    Tests discover Granules
    """

    def setUp(self, temp=None) -> None:
        provider = {
            "host": "data.remss.com",
            "protocol": "https"
        }

        granule_id_extraction = "^(f16_\\d{8}v7.gz)$"
        provider_path = "/ssmi/f16/bmaps_v07/y2021/"
        discover_tf = {
            "depth": 0,
            "dir_reg_ex": ".*"
        }
        event = configure_event(provider, granule_id_extraction, provider_path, discover_tf)
        self.dg = DiscoverGranulesHTTP(event)

    def configure_mock_session(self, mock_session, provider):
        mock_session.get.return_value = FakeResponse(self.get_html(provider))
        fhrs = []
        for x in self.get_header_responses(provider):
            fhrs.append(FakeHeadResponse(x))
        mock_session.head.side_effect = fhrs

    @staticmethod
    def get_html(provider):
        with open(os.path.join(THIS_DIR, f'test_page_{provider}.html'), 'r', encoding='UTF-8') as test_html_file:
            return test_html_file.read()

    @staticmethod
    def get_header_responses(provider):
        with open(os.path.join(THIS_DIR, f'head_responses_{provider}.json'), 'r', encoding='UTF-8') as test_file:
            return json.load(test_file)['head_responses']

    @staticmethod
    def get_sample_event(event_type='skip'):
        with open(os.path.join(THIS_DIR, f'input_event_{event_type}.json'), 'r', encoding='UTF-8') as test_event_file:
            return json.load(test_event_file)

    @patch('requests.Session')
    def test_discover_granules(self, mock_session):
        self.dg.discover = MagicMock()
        self.dg.discover_granules()
        self.assertTrue(mock_session.called)
        self.assertTrue(self.dg.discover.called)

    @patch('requests.Session')
    def test_get_file_link_remss_without_regex(self, mock_session):
        self.configure_mock_session(mock_session, 'remss')
        self.dg.file_reg_ex = ''
        discover_count = self.dg.discover(mock_session, {})
        self.assertEqual(discover_count, 3)

    @patch('requests.Session')
    def test_get_file_link_remss_with_regex(self, mock_session):
        self.configure_mock_session(mock_session, 'remss')
        self.dg.file_reg_ex = "f16_20200402v7.gz"
        discover_count = self.dg.discover(mock_session, {})
        self.assertEqual(discover_count, 1)

    @patch('requests.Session')
    def test_get_file_link_amsu_without_regex(self, mock_session):
        self.configure_mock_session(mock_session, 'msut')
        self.dg.granule_id_extraction = '((tlt|uah).*_6\\.0(\\.nc)?)'
        self.dg.file_reg_ex = '((tlt|uah).*_6\\.0(\\.nc)?)'
        discover_count = self.dg.discover(mock_session, {})
        self.assertEqual(discover_count, 4)

    @patch('requests.Session')
    def test_get_file_link_amsu_with_regex(self, mock_session):
        self.configure_mock_session(mock_session, 'msut')
        self.dg.granule_id_extraction = '((tlt|uah).*_6\\.0(\\.nc)?)'
        self.dg.file_reg_ex = 'tltglhmam_6\\.0$'
        discover_count = self.dg.discover(mock_session, {})
        self.assertEqual(discover_count, 1)


if __name__ == "__main__":
    unittest.main()
