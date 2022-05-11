import os

from task.discover_granules_base import DiscoverGranulesBase
from unittest.mock import MagicMock, patch
import logging
import unittest
from .helpers import get_event

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):

    @patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
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

    def test_get_path(self):
        self.dg.provider['host'] = 'host'
        self.dg.provider['protocol'] = 'protocol'
        path = 'protocol://host/some/path/and/file'
        ret_dict = self.dg.get_path(path)
        self.assertEqual(ret_dict.get('path'), 'some/path/and')
        self.assertEqual(ret_dict.get('name'), 'file')

    def test_populate_dict(self):
        key = 'key'
        etag = 'ETag'
        last_mod = 'Last-Modified'
        size = 'Size'
        td = {}
        self.dg.populate_dict(target_dict=td, key=key, etag=etag, last_mod=last_mod, size=size)
        self.assertIn(key, td)
        self.assertIn(etag, td['key'])
        self.assertIn(last_mod, td['key'])
        self.assertIn(size, td['key'])

    def test_update_etag_lm(self):
        d1 = {'key1': {'ETag': 'etag1', 'Last-Modified': 'lm1', 'Size': 's1'}}
        d2 = {}
        self.dg.update_etag_lm(d2, d1, 'key1')
        self.assertDictEqual(d1, d2)


if __name__ == "__main__":
    unittest.main()
