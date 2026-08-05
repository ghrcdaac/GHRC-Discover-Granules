import datetime
import os
import time
import pytest
import json
from dateutil.tz import tzutc

from task.discover_granules_s3 import DiscoverGranulesS3, get_secret_value, get_s3_client, get_s3_client_from_secret, \
    ONE_MEBIBIT


class FakeContext:
    @staticmethod
    def get_remaining_time_in_millis():
        return time.time()


@pytest.fixture(scope="function")
def discover_granules_s3(get_event):
    def gen_dg_s3(event_protocol):
        event = get_event(event_protocol)
        return DiscoverGranulesS3(event, context=FakeContext())
    
    return gen_dg_s3


def test_get_secret_value(mocker):
    mock_secrets = mocker.patch('boto3.client')
    mock_secrets.get_secret_value.return_value = {
        'SecretString': '{\"aws_access_key_id\": \"aws_access_key_id\", \"aws_secret_access_key\": \"aws_secret_access_key\"}'
    }
    ret = get_secret_value('fake_secret_manager', mock_secrets)
    assert 'aws_access_key_id' in ret and 'aws_secret_access_key' in ret


def test_get_secret_value_failure(mocker):
    mock_secrets = mocker.patch('boto3.client')
    mock_secrets.get_secret_value.return_value = {
        'SecretString': 'unexpected secret string'
    }
    with pytest.raises((json.JSONDecodeError, TypeError)):
        ret = get_secret_value('fake_secret_manager', mock_secrets)


def test_get_s3_client(mocker):
    test_client = get_s3_client()
    assert test_client is not None


def test_get_s3_client_from_secret(mocker):
    mock_client = mocker.patch('boto3.client')
    mock_get_secrets = mocker.patch('task.discover_granules_s3.get_secret_value')
    mock_get_secrets.return_value = {
        "aws_access_key_id": "aws_access_key_id",
        "aws_secret_access_key": "aws_secret_access_key"
    }
    test_client = get_s3_client_from_secret('fake_secret_manager')
    assert test_client is not None


def test_discover_granules_s3(discover_granules_s3):
    dg = discover_granules_s3('skip_s3')
    test_resp_iter = [
        {
            'Contents': [
                {
                    'Key': 'key/key1',
                    'ETag': 'etag1',
                    'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                    'Size': 1
                },
                {
                    'Key': 'key/key2',
                    'ETag': 'etag2',
                    'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                    'Size': 2
                }
            ]
        }
    ]

    dg.discover(test_resp_iter)
    discover_count = len(dg.dbm.list_dict)
    assert discover_count == 2


def test_discover_granules_s3_file_regex(discover_granules_s3):
    dg = discover_granules_s3('skip_s3')
    dg.granule_id_extraction = '(key1.txt)'
    dg.dir_reg_ex = None
    test_resp_iter = [
        {
            'Contents': [
                {
                    'Key': 'key/key1.txt',
                    'ETag': 'etag1',
                    'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                    'Size': 1
                },
                {
                    'Key': 'key/key2.txt',
                    'ETag': 'etag2',
                    'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                    'Size': 2
                }
            ]
        }
    ]

    dg.discover(test_resp_iter)
    discover_count = len(dg.dbm.list_dict)
    assert discover_count == 1


def test_discover_granules_s3_dir_regex(discover_granules_s3):
    dg = discover_granules_s3('skip_s3')
    # dg.granule_id_extraction = ''
    dg.dir_reg_ex = 'key1'
    test_resp_iter = [
        {
            'Contents': [
                {
                    'Key': 'key1/key1.txt',
                    'ETag': 'etag1',
                    'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                    'Size': 1
                },
                {
                    'Key': 'key2/key2.txt',
                    'ETag': 'etag2',
                    'LastModified': datetime.datetime(2020, 8, 14, 17, 19, 34, tzinfo=tzutc()),
                    'Size': 1
                }
            ]
        }
    ]

    dg.discover(test_resp_iter)
    discover_count = len(dg.dbm.list_dict)
    assert discover_count == 1


def test_move_granule(mocker, discover_granules_s3):
    mock_client = mocker.patch('boto3.client')
    dg = discover_granules_s3('skip_s3')
    dg_spy = mocker.spy(dg, 'multipart_upload')
    os.environ['stackName'] = 'unit-test'
    granule_dict = {
        'name': 's3://some_provider/at/a/path/that/is/fake.txt',
        'size': ONE_MEBIBIT
    }
    dg.move_granule(mock_client, mock_client, granule_dict)

    assert mock_client.put_object.call_count == 1


def test_move_granule_multipart(mocker, discover_granules_s3):
    mock_client = mocker.patch('boto3.client')
    dg = discover_granules_s3('skip_s3')
    dg_spy = mocker.spy(dg, 'multipart_upload')
    os.environ['stackName'] = 'unit-test'
    granule_dict = {
        'name': 's3://some_provider/at/a/path/that/is/fake.txt',
        'size': (ONE_MEBIBIT * 8)
    }
    dg.move_granule(mock_client, mock_client, granule_dict)

    assert dg_spy.call_count == 1


def test_move_granule_wrapper(mocker, discover_granules_s3):
    mock_client = mocker.patch('boto3.client')
    mock_get_secrets = mocker.patch('task.discover_granules_s3.get_secret_value')
    mock_get_secrets.return_value = {
        "aws_access_key_id": "aws_access_key_id",
        "aws_secret_access_key": "aws_secret_access_key"
    }
    dg = discover_granules_s3('skip_s3')
    mocker.patch.object(dg, 'move_granule')
    test_list_dict = [
        {
            'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_000000.dat',
            'ETag': 'ec5273963f74811028e38a367beaf7a5', 'Last-Modified': '1645564956.0', 'Size': 4553538
        },
        {
            'name': 's3://sharedsbx-private/lma/nalma/raw/short_test/LA_NALMA_firetower_211130_001000.dat',
            'ETag': '919a1ba1dfbbd417a662ab686a2ff574', 'Last-Modified': '1645564956.0', 'Size': 4706838
        }
    ]

    dg.move_granule_wrapper(test_list_dict)
    assert dg.move_granule.call_count == 2
