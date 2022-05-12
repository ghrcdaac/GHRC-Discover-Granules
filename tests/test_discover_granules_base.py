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

    def test_generate_cumulus_output(self):
        test_dict = {
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat': {
                'ETag': 'ec5273963f74811028e38a367beaf7a5', 'Last-Modified': '1645564956.0', 'Size': 4553538},
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat': {
                'ETag': '919a1ba1dfbbd417a662ab686a2ff574', 'Last-Modified': '1645564956.0', 'Size': 4706838}}
        mapping = {'^.*_NALMA_.*\\.cmr\\.(xml|json)$': {'bucket': 'public', 'lzards': None},
                   '^.*_NALMA_.*(\\.gz)$': {'bucket': 'protected', 'lzards': None},
                   '^.*_NALMA_.*(\\.dat)$': {'bucket': 'private', 'lzards': None}}

        ret_list = [self.dg.generate_cumulus_record(k, v, mapping) for k, v in test_dict.items()]

        expected_entries = [
            {'granuleId': 'LA_NALMA_firetower_211130_000000.dat', 'dataType': 'rssmif16d', 'version': '7', 'files': [
                {'bucket': 'ghrcsbxw-private', 'checksum': '', 'checksumType': '',
                 'filename': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat',
                 'name': 'LA_NALMA_firetower_211130_000000.dat',
                 'path': 's3://sharedsbx-private/lma/nalma/raw/short_test', 'size': 4553538, 'time': '1645564956.0',
                 'type': ''}]},
            {'granuleId': 'LA_NALMA_firetower_211130_001000.dat', 'dataType': 'rssmif16d', 'version': '7', 'files': [
                {'bucket': 'ghrcsbxw-private', 'checksum': '', 'checksumType': '',
                 'filename': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat',
                 'name': 'LA_NALMA_firetower_211130_001000.dat',
                 'path': 's3://sharedsbx-private/lma/nalma/raw/short_test', 'size': 4706838, 'time': '1645564956.0',
                 'type': ''}]}
        ]

        for x in expected_entries:
            self.assertIn(x, ret_list)

    def test_generate_cumulus_output_lzards(self):
        test_dict = {
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat': {
                'ETag': 'ec5273963f74811028e38a367beaf7a5', 'Last-Modified': '1645564956.0', 'Size': 4553538},
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat': {
                'ETag': '919a1ba1dfbbd417a662ab686a2ff574', 'Last-Modified': '1645564956.0', 'Size': 4706838}}
        mapping = {'^.*_NALMA_.*\\.cmr\\.(xml|json)$': {'bucket': 'public', 'lzards': None},
                   '^.*_NALMA_.*(\\.gz)$': {'bucket': 'protected', 'lzards': None},
                   '^.*_NALMA_.*(\\.dat)$': {'bucket': 'private', 'lzards': True}}

        ret_list = [self.dg.generate_cumulus_record(k, v, mapping) for k, v in test_dict.items()]

        expected_entries = [
            {'granuleId': 'LA_NALMA_firetower_211130_000000.dat', 'dataType': 'rssmif16d', 'version': '7', 'files': [
                {'bucket': 'ghrcsbxw-private', 'checksum': 'ec5273963f74811028e38a367beaf7a5', 'checksumType': 'md5',
                 'filename': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat',
                 'name': 'LA_NALMA_firetower_211130_000000.dat',
                 'path': 's3://sharedsbx-private/lma/nalma/raw/short_test', 'size': 4553538, 'time': '1645564956.0',
                 'type': ''}]},
            {'granuleId': 'LA_NALMA_firetower_211130_001000.dat', 'dataType': 'rssmif16d', 'version': '7', 'files': [
                {'bucket': 'ghrcsbxw-private', 'checksum': '919a1ba1dfbbd417a662ab686a2ff574', 'checksumType': 'md5',
                 'filename': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat',
                 'name': 'LA_NALMA_firetower_211130_001000.dat',
                 'path': 's3://sharedsbx-private/lma/nalma/raw/short_test', 'size': 4706838, 'time': '1645564956.0',
                 'type': ''}]}]

        for x in expected_entries:
            self.assertIn(x, ret_list)

    def test_discover_granules(self):
        self.assertRaises(NotImplementedError, self.dg.discover_granules)


if __name__ == "__main__":
    unittest.main()
