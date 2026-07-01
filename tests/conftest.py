import os
import json
import psycopg2
import pytest

from task.dbm_postgresql import get_db_manager_psql

@pytest.fixture(scope="session")
def mock_cumulus_dbm():
    class MockCumulusDBM(object):
        def filter_cross_collection_duplicates(self, granule_list_dict):
            return set()
        def filter_against_cumulus(self, granule_list_dict):
            return set()
        def flush_dict(self):
            pass
        def read_batch(self):
            return {}
        def trim_results(self):
            return []
        def close_db(self):
            pass

    return MockCumulusDBM()


@pytest.fixture(scope="function", autouse=True)
def get_mock_cumulus_dbm(mocker, mock_cumulus_dbm):
    mocked_get_db_manager_cumulus = mocker.patch('task.dbm_get.get_db_manager_cumulus')
    mocked_get_db_manager_cumulus.return_value = mock_cumulus_dbm
    yield mocked_get_db_manager_cumulus


def is_db_ready(docker_ip, port):
    try:
        with psycopg2.connect(dbname='pytest', user='pytest', password='pytest', host=docker_ip, port=port) as db:
            pass
        return True
    except psycopg2.OperationalError:
        return False
    

@pytest.fixture(scope="session", autouse=True)
def postgresql_service(docker_ip, docker_services, mock_cumulus_dbm, session_mocker):
    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("psql_db", 5432)
    docker_services.wait_until_responsive(
        timeout=60.0, pause=0.1, check=lambda: is_db_ready(docker_ip, port)
    )

    orig_get_db_manager_psql = get_db_manager_psql

    def mock_get_db_manager_psql(**kwargs):
        test_args = {
            'database': 'pytest',
            'user': 'pytest',
            'password': 'pytest',
            'host': docker_ip,
            'port': port,
            'collection_id': 'test_id___1',
            'provider_url': 'protocol://host/path/',
            'batch_limit': 100,
            'cumulus_dbm': mock_cumulus_dbm,
        }
        dbm = orig_get_db_manager_psql(**test_args)
        return dbm

    session_mocker.patch('task.dbm_get.get_db_manager_psql', side_effect=mock_get_db_manager_psql)


@pytest.fixture(scope="function")
def get_event():
    def event_dict(event_protocol):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(f'{dir_path}/input_event_{event_protocol}.json', 'r', encoding='utf-8') as file:
            json_dict = json.load(file)
        return json_dict
    
    return event_dict