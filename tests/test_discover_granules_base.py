import os
import json
import pytest

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex


@pytest.fixture(scope="function")
def get_event():
    def event_dict(event_protocol):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(f'{dir_path}/event_{event_protocol}.json', 'r', encoding='utf-8') as file:
            json_dict = json.load(file)
        return json_dict
    
    return event_dict


def test_generate_cumulus_output(mocker, get_event):
    event = get_event('s3')
    mocker.patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
    mock_time = mocker.patch('time.time', return_value=0)

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

    ret_list = dg.generate_cumulus_output(test_dict)
    print(f'ret_list:\n {ret_list}')

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
        assert val in ret_list


def test_generate_lzards_output(mocker, get_event):
    event = get_event('s3')
    mocker.patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
    
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

    ret_list = dg.lzards_output_generator(test_dict)

    for val in ret_list:
        for key in ['granuleId', 'dataType', 'version', 'files']:
            assert val.get(key) is not None


def test_generate_lambda_output_lzards_called(mocker, get_event):
    event = get_event('s3')
    mocker.patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
    mock_lzards_output_generator = mocker.patch.object(dg, 'lzards_output_generator', return_value=[])
    mock_lzards_output_generator.return_value = []

    dg.config['workflow_name'] = 'LZARDSBackup'
    dg.generate_lambda_output({})
    mock_lzards_output_generator.assert_called()


def test_generate_lambda_output_cumulus_called(mocker, get_event):
    event = get_event('s3')
    mocker.patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
    mock_generate_cumulus_output = mocker.patch.object(dg, 'generate_cumulus_output', return_value=[])

    dg.config['workflow_name'] = 'DiscoverGranules'
    dg.generate_lambda_output({})
    mock_generate_cumulus_output.assert_called()


def test_discover_granules(mocker, get_event):
    event = get_event('s3')
    mocker.patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
    with pytest.raises(NotImplementedError):
        dg.discover_granules()


def test_check_reg_ex_match():
    assert check_reg_ex(r'.*', 'test_text')


def test_check_reg_ex_no_match():
    assert not check_reg_ex(r'No_match', 'test_text')


def test_check_reg_ex_none():
    assert check_reg_ex(None, 'test_text')


def test_generate_cumulus_output_multi_file_granules(mocker, get_event):
    event = get_event('s3_multi_file_granule')
    mocker.patch.multiple(DiscoverGranulesBase, __abstractmethods__=set())
    dg = DiscoverGranulesBase(event)  # pylint: disable=abstract-class-instantiated
    mock_time = mocker.patch('time.time', return_value=0)

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

    ret_list = dg.generate_cumulus_output(test_dict)
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

    assert ret_list == expected
