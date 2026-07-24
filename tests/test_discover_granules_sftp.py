import os
import pytest

import task.discover_granules_sftp as sftp


class SFTPTestFile:
    def __init__(self, filename, file_type, mod_time, size):
        self.filename = filename
        self.file_type = file_type
        self.st_mtime = mod_time
        self.st_size = size

    def stat(self):
        return self

    def __str__(self):
        return f'{self.filename}'


class SFTPTestClient:
    def __init__(self, path, dir_count, file_count):
        self.listdir_resp = self.listdir_setup(path, dir_count, file_count)

    def listdir_setup(self, path, dir_count, file_count):
        resp = []
        for x in range(dir_count):
            resp.append(SFTPTestFile(f'dir_{x}', 'dir', 1, 1))

        for x in range(file_count):
            resp.append(SFTPTestFile(f'file_{x}', 'file', 1, 1))

        return resp

    def chdir(self, path):
        pass

    def listdir(self):
        return self.listdir_resp

    def stat(self, sftp_test_file):
        return sftp_test_file.stat()


class FakeKms:
    def __init__(self, CiphertextBlob):
        self.rsp = {'Plaintext': CiphertextBlob}

    def decrypt(self, CiphertextBlob, KeyId):
        return self.rsp


@pytest.fixture(scope="function")
def discover_granules_sftp(get_event):
    def gen_dg_sftp(event_protocol):
        event = get_event(event_protocol)
        return sftp.DiscoverGranulesSFTP(event, None)
    
    return gen_dg_sftp


def test_discover_granules(mocker, discover_granules_sftp):
    mock_create_config = mocker.patch('task.discover_granules_sftp.create_ssh_sftp_config')
    mock_setup_config = mocker.patch('task.discover_granules_sftp.setup_ssh_sftp_client')
    dg = discover_granules_sftp('sftp')
    dg_spy = mocker.spy(dg, 'discover')
    dg.discover_granules()

    assert dg_spy.call_count == 1
    assert mock_create_config.call_count == 1
    assert mock_setup_config.call_count == 1


def test_discover_granules_sftp(discover_granules_sftp, get_event):
    event = get_event('sftp')
    sftp_test_client = SFTPTestClient(event.get('config').get('provider_path'), 3, 3)
    dg = discover_granules_sftp('sftp')
    dg.discover(sftp_test_client)

    discover_count = len(dg.dbm.list_dict)
    assert discover_count  == 3


def test_discover_granules_sftp_recursion(mocker, discover_granules_sftp, get_event):
    mock_regex = mocker.patch('re.search')
    event = get_event('sftp')
    event.get('config').get('collection').get('meta').get('discover_tf')['depth'] = 1
    sftp_test_client = SFTPTestClient(event.get('config').get('provider_path'), 3, 0)
    dg = discover_granules_sftp('sftp')
    dg.discover(sftp_test_client)

    discover_count = len(dg.dbm.list_dict)
    assert discover_count == 0


def test_discover_granules_sftp_no_reg_ex_match(mocker, discover_granules_sftp, get_event):
    mock_regex = mocker.patch('re.search', return_value=False)
    event = get_event('sftp')
    event.get('config').get('collection').get('meta').get('discover_tf')['depth'] = 1
    sftp_test_client = SFTPTestClient(event.get('config').get('provider_path'), 3, 0)
    dg = discover_granules_sftp('sftp')
    dg.discover(sftp_test_client)

    discover_count = len(dg.dbm.list_dict)
    assert discover_count == 0


def test_setup_ssh_sftp_client(mocker):
    mock_client = mocker.patch('paramiko.SSHClient')
    sftp_client = sftp.setup_ssh_sftp_client()
    assert sftp_client is not None


def test_create_sftp_config(mocker):
    mock_get_pkey = mocker.patch('task.discover_granules_sftp.get_private_key')
    mock_decrypt = mocker.patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    uname = 'username'
    pword = 'password'
    mock_get_pkey.side_effect = ['privateKey']
    mock_decrypt.side_effect = [uname, pword]
    config_params = {
        'host': 'host',
        'port': 22,
        'username': uname,
        'password': pword,
        'privateKey': 'privateKey',
        'key_filename': 'key_filename'
    }
    
    res = sftp.create_ssh_sftp_config(**config_params)
    config_values = config_params.values()
    response_values = res.values()

    for config_value, response_value in zip(config_values, response_values):
        assert config_value == response_value


def test_create_sftp_config_unset_params(mocker):
    mock_decrypt = mocker.patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    uname = 'username'
    pword = 'password'
    mock_decrypt.side_effect = [uname, pword]
    config_params = {
        'host': 'host',
        'port': 22,
        'username': uname,
        'password': pword,
        'private_key': None,
        'key_filename': None
    }

    res = sftp.create_ssh_sftp_config(**config_params)
    config_values = config_params.values()
    response_values = res.values()

    for config_value, response_value in zip(config_values, response_values):
        assert config_value == response_value


def test_kms_decrypt_ciphertext():
    os.environ['AWS_DECRYPT_KEY_ARN'] = 'fake_arn'
    t = b'test_text'
    kms_client = FakeKms(t)
    res = sftp.kms_decrypt_ciphertext(t, kms_client)

    assert t.decode() == res


def test_kms_decrypt_ciphertext_2(mocker):
    mock_client = mocker.patch('boto3.client')
    os.environ['AWS_DECRYPT_KEY_ARN'] = 'fake_arn'
    t = b'test_text'
    mock_client.side_effect = [FakeKms(t)]
    res = sftp.kms_decrypt_ciphertext(t)

    assert t.decode() == res


def test_get_private_key(mocker):
    mock_rsa = mocker.patch('paramiko.rsakey.RSAKey.from_private_key')
    mock_client = mocker.patch('boto3.client')
    temp_file = os.getcwd()
    fake_file = f'{temp_file}/fake_key'
    with open(fake_file, 'w+', encoding='utf-8') as _:
        pass
    sftp.get_private_key('fake_key', temp_file)
    assert mock_rsa.call_count == 1



def test_decrypt_credential_none(mocker):
    mock_decrypt = mocker.patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    username = None
    mock_decrypt.side_effect = [username]
    sftp.decrypt_credential(username, True)

    assert mock_decrypt.call_count == 0


def test_decrypt_credential(mocker):
    mock_decrypt = mocker.patch('task.discover_granules_sftp.kms_decrypt_ciphertext')
    username = 'something'
    mock_decrypt.side_effect = [username]
    sftp.decrypt_credential(username, True)

    assert mock_decrypt.call_count == 1
