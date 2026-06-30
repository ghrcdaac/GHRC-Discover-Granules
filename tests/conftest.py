import os
import json
import pytest

@pytest.fixture(scope="session")
def mock_cumulus_dbm(session_mocker):
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


@pytest.fixture(scope="function")
def get_event():
    def event_dict(event_protocol):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(f'{dir_path}/input_event_{event_protocol}.json', 'r', encoding='utf-8') as file:
            json_dict = json.load(file)
        return json_dict
    
    return event_dict