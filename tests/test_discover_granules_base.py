import os

from unittest.mock import MagicMock, patch
import logging
import unittest
from task.discover_granules_base import DiscoverGranulesBase
from .helpers import get_event

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):
    """
    Tests discover Granules
    """

    @patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    def setUp(self) -> None:
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
        event = get_event(provider, granule_id_extraction, provider_path, discover_tf)
        self.dg = DiscoverGranulesBase(event, logging)
        self.dg.get_session = MagicMock()

    def test_populate_dict(self):
        key = 'key'
        etag = 'ETag'
        last_mod = 'Last-Modified'
        size = 'Size'
        t_dict = {}
        self.dg.populate_dict(target_dict=t_dict, key=key, etag=etag, last_mod=last_mod, size=size)
        self.assertIn(key, t_dict)
        self.assertIn(etag, t_dict['key'])
        self.assertIn(last_mod, t_dict['key'])
        self.assertIn(size, t_dict['key'])

    def test_update_etag_lm(self):
        dict1 = {'key1': {'ETag': 'etag1', 'Last-Modified': 'lm1', 'Size': 's1'}}
        dict2 = {}
        self.dg.update_etag_lm(dict2, dict1, 'key1')
        self.assertDictEqual(dict1, dict2)

    def test_generate_cumulus_output(self):
        test_dict = {
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat': {
                'ETag': 'ec5273963f74811028e38a367beaf7a5', 'Last-Modified': '1645564956.0', 'Size': 4553538},
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat': {
                'ETag': '919a1ba1dfbbd417a662ab686a2ff574', 'Last-Modified': '1645564956.0', 'Size': 4706838}}

        ret_list = self.dg.generate_cumulus_output_new(test_dict)

        expected_entries = [
            {
                'granuleId': 'LA_NALMA_firetower_211130_000000.dat',
                'dataType': 'rssmif16d',
                'version': '7',
                'files': [
                    {
                        'name': 'LA_NALMA_firetower_211130_000000.dat',
                        'path': 's3://sharedsbx-private/lma/nalma/raw/short_test',
                        'type': ''
                    }
                ]
            },
            {
                'granuleId': 'LA_NALMA_firetower_211130_001000.dat',
                'dataType': 'rssmif16d',
                'version': '7',
                'files': [
                    {
                        'name': 'LA_NALMA_firetower_211130_001000.dat',
                        'path': 's3://sharedsbx-private/lma/nalma/raw/short_test',
                        'type': ''
                    }
                ]
            }
        ]

        for val in expected_entries:
            self.assertIn(val, ret_list)

    def test_generate_lambda_output_lzards_called(self):
        self.dg.lzards_output_generator = MagicMock()
        self.dg.config['workflow_name'] = 'LZARDSBackup'
        self.dg.generate_lambda_output({})
        assert self.dg.lzards_output_generator.called

    def test_generate_lambda_output_cumulus_called(self):
        self.dg.generate_cumulus_output_new = MagicMock()
        self.dg.config['workflow_name'] = 'DiscoverGranules'
        self.dg.generate_lambda_output({})
        assert self.dg.generate_cumulus_output_new.called

    def test_generate_lzards_output(self):
        test_dict = {
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat': {
                'ETag': 'ec5273963f74811028e38a367beaf7a5', 'Last-Modified': '1645564956.0', 'Size': 4553538},
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat': {
                'ETag': '919a1ba1dfbbd417a662ab686a2ff574', 'Last-Modified': '1645564956.0', 'Size': 4706838}}

        ret_list = self.dg.lzards_output_generator(test_dict)

        expected_entries = [
            {
                'granuleId': 'LA_NALMA_firetower_211130_000000.dat',
                'dataType': 'rssmif16d',
                'version': '7',
                'files': [
                    {
                        'bucket': 'ghrcsbxw-private',
                        'checksum': 'ec5273963f74811028e38a367beaf7a5',
                        'checksumType': 'md5',
                        'key': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat',
                        'size': 4553538,
                        'source': '',
                        'type': ''
                    }
                ]
            },
            {
                'granuleId': 'LA_NALMA_firetower_211130_001000.dat',
                'dataType': 'rssmif16d',
                'version': '7',
                'files': [
                    {
                        'bucket': 'ghrcsbxw-private',
                        'checksum': '919a1ba1dfbbd417a662ab686a2ff574',
                        'checksumType': 'md5',
                        'key': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat',
                        'size': 4706838,
                        'source': '',
                        'type': ''
                    }
                ]
            }
        ]

        for val in expected_entries:
            self.assertIn(val, ret_list)

    def test_discover_granules(self):
        self.assertRaises(NotImplementedError, self.dg.discover_granules)


if __name__ == "__main__":
    unittest.main()
