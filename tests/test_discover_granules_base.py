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

    @mock.patch('time.time', mock.MagicMock(return_value=0))
    def test_generate_cumulus_output(self):
        test_dict = [
            {
                'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat',
                'etag': 'ec5273963f74811028e38a367beaf7a5', 'last_modified': '1645564956.0', 'size': 4553538
            },
            {
                'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat',
                'etag': '919a1ba1dfbbd417a662ab686a2ff574', 'last_modified': '1645564956.0', 'size': 4706838
            }
        ]

        ret_list = self.dg.generate_cumulus_output(test_dict)

        expected_entries = []
        for granule in test_dict:
            expected_entries.append(
                {
                    'granuleId': str(granule.get('name')).rsplit('/', maxsplit=1)[-1],
                    'dataType': 'nalmaraw',
                    'version': '1',
                    'files': [
                        {
                            'name': str(granule.get('name')).rsplit('/', maxsplit=1)[-1],
                            'path': 'lma/nalma/raw/short_test',
                            'size': granule.get('size'),
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
        test_dict = [
            {
                'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat',
                'etag': 'ec5273963f74811028e38a367beaf7a5', 'last_modified': '1645564956.0', 'size': 4553538,
                'collectionId': 'test_collection___1'
            },
            {
                'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat',
                'etag': '919a1ba1dfbbd417a662ab686a2ff574', 'last_modified': '1645564956.0', 'size': 4706838,
                'collectionId': 'test_collection___1'
            }
        ]

        ret_list = self.dg.lzards_output_generator(test_dict)

        for val in ret_list:
            for key in ['granuleId', 'dataType', 'version', 'files']:
                self.assertIsNotNone((val.get(key)))

    def test_discover_granules(self):
        self.assertRaises(NotImplementedError, self.dg.discover_granules)

    def test_check_reg_ex_match(self):
        self.assertTrue(check_reg_ex(r'.*', 'test_text'))

    def test_check_reg_ex_no_match(self):
        self.assertFalse(check_reg_ex(r'No_match', 'test_text'))

    def test_check_reg_ex_none(self):
        self.assertTrue(check_reg_ex(None, 'test_text'))


class TestDiscoverGranulesMultiFile(unittest.TestCase):
    """
    Tests discover Granules
    """

    @patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    def setUp(self) -> None:
        event = get_event('s3_multi_file_granule')
        self.dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
        self.dg.get_session = MagicMock()

    @mock.patch('time.time', mock.MagicMock(return_value=0))
    def test_generate_cumulus_output_multi_file_granules(self):
        test_dict = [
            {
                'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.file_1.dat',
                'etag': 'ec5273963f74811028e38a367beaf7a5', 'last_modified': '1645564956.0', 'size': 4553538
            },
            {
                'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.file_2.dat',
                'etag': '919a1ba1dfbbd417a662ab686a2ff574', 'last_modified': '1645564956.0', 'size': 4706838
            }
        ]

        ret_list = self.dg.generate_cumulus_output(test_dict)
        print(f'ret_list: {ret_list}')

        expected = [
            {
                'granuleId': 'LA_NALMA_firetower_211130_000000',
                'dataType': 'nalmaraw',
                'version': '1',
                'files': [
                    {
                        'name': 'LA_NALMA_firetower_211130_000000.file_1.dat',
                        'path': 'lma/nalma/raw/short_test',
                        'size': 4553538,
                        'time': 0,
                        'url_path': 'nalmaraw__1',
                        'bucket': 'sharedsbx-private',
                        'type': ''
                    },
                    {
                        'name': 'LA_NALMA_firetower_211130_000000.file_2.dat',
                        'path': 'lma/nalma/raw/short_test',
                        'size': 4706838,
                        'time': 0,
                        'url_path': 'nalmaraw__1',
                        'bucket': 'sharedsbx-private',
                        'type': ''
                    }
                ]
            }
        ]

        self.assertEqual(ret_list, expected)


if __name__ == "__main__":
    unittest.main()
