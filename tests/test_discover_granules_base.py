import os
from unittest import mock

from unittest.mock import MagicMock, patch
import unittest
from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from .helpers import get_event

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):
    """
    Tests discover Granules
    """

    @patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    def setUp(self) -> None:
        event = get_event('s3')
        self.dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
        self.dg.get_session = MagicMock()

    def test_populate_dict(self):
        key = 'key'
        etag = 'ETag'
        last_mod = 'Last-Modified'
        granule_id = 'GranuleId'
        collection_id = 'CollectionId'
        size = 'Size'
        t_dict = {}
        self.dg.populate_dict(
            target_dict=t_dict,
            key=key, etag=etag,
            granule_id=granule_id,
            collection_id=collection_id,
            last_mod=last_mod,
            size=size
        )
        self.assertIn(key, t_dict)
        self.assertIn(etag, t_dict['key'])
        self.assertIn(granule_id, t_dict['key'])
        self.assertIn(last_mod, t_dict['key'])
        self.assertIn(size, t_dict['key'])

    def test_update_etag_lm(self):
        dict1 = {
            'key1': {
                'ETag': 'etag1', 'GranuleId': 'granule_id1',
                'CollectionId': 'collection_id1',
                'Last-Modified': 'lm1', 'Size': 's1'
            }
        }
        dict2 = {}
        self.dg.update_etag_lm(dict2, dict1, 'key1')
        self.assertDictEqual(dict1, dict2)

    @mock.patch('time.time', mock.MagicMock(return_value=0))
    def test_generate_cumulus_output(self):
        test_dict = {
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat': {
                'ETag': 'ec5273963f74811028e38a367beaf7a5', 'Last-Modified': '1645564956.0', 'Size': 4553538},
            's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat': {
                'ETag': '919a1ba1dfbbd417a662ab686a2ff574', 'Last-Modified': '1645564956.0', 'Size': 4706838}}

        ret_list = self.dg.generate_cumulus_output(test_dict)

        expected_entries = []
        for k, v in test_dict:
            expected_entries.append(
                {
                    'granuleId': str(k).rsplit('/', maxsplit=1),
                    'dataType': 'nalmaraw',
                    'version': '1',
                    'files': [
                        {
                            'name': str(k).rsplit('/', maxsplit=1),
                            'path': 'lma/nalma/raw/short_test',
                            'size': v.get('Size'),
                            'time': 0,
                            'url_path': 'nalmaraw__1',
                            'bucket': 'sharedsbx-private',
                            'type': ''
                        }
                    ]
                }
            )

        for val in expected_entries:
            self.assertIn(val, ret_list)

    def test_generate_lambda_output_lzards_called(self):
        self.dg.lzards_output_generator = MagicMock()
        self.dg.config['workflow_name'] = 'LZARDSBackup'
        self.dg.generate_lambda_output({})
        assert self.dg.lzards_output_generator.called

    def test_generate_lambda_output_cumulus_called(self):
        self.dg.generate_cumulus_output = MagicMock()
        self.dg.config['workflow_name'] = 'DiscoverGranules'
        self.dg.generate_lambda_output({})
        assert self.dg.generate_cumulus_output.called

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
                'dataType': 'nalmaraw',
                'version': '1',
                'files': [
                    {
                        'bucket': 'sharedsbx-private',
                        'key': 'lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat',
                        'size': 4553538,
                        'source': '',
                        'type': ''
                    }
                ]
            },
            {
                'granuleId': 'LA_NALMA_firetower_211130_001000.dat',
                'dataType': 'nalmaraw',
                'version': '1',
                'files': [
                    {
                        'bucket': 'sharedsbx-private',
                        'key': 'lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat',
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

    def test_check_reg_ex_match(self):
        self.assertTrue(check_reg_ex(r'.*', 'test_text'))

    def test_check_reg_ex_no_match(self):
        self.assertFalse(check_reg_ex(r'No_match', 'test_text'))

    def test_check_reg_ex_none(self):
        self.assertTrue(check_reg_ex(None, 'test_text'))


if __name__ == "__main__":
    unittest.main()
