import os
import unittest
from unittest.mock import patch


class TestDGMImportsCumulus(unittest.TestCase):
    @patch('psycopg2.connect')
    @patch('json.loads')
    @patch('boto3.client')
    def test_cumulus_config(self, mock_client, mock_json_load, mock_psycopg2):
        mock_client.get_secret_value.return_value = {}
        mock_json_load.return_value = {'username': 'fake_username'}
        os.environ['db_type'] = 'postgresql'
        from task.dgm import get_db_manager, DBManager
        dbm = get_db_manager(db_type='postgresql')
        self.assertIsInstance(dbm, DBManager)



class TestDGMCumulusDBM(unittest.TestCase):
    @patch('psycopg2.connect')
    @patch('json.loads')
    @patch('boto3.client')
    def setUp(self, mock_client, mock_json_load, mock_psycopg2) -> None:
        mock_client.get_secret_value.return_value = {}
        mock_json_load.return_value = {'username': 'fake_username'}
        os.environ['db_type'] = 'postgresql'
        from task.dgm import get_db_manager, DBManager
        self.dbm = get_db_manager(db_type='postgresql')

    def test_dbm_postgresql(self):
        self.dbm.close_db()

    def test_dbm_postgresql_2(self):
        test_record = {
            'name': 'test_name', 'granule_id': 'test_granule_id', 'collection_id': 'test_collection_id',
            'etag': 'test_etag', 'last_modified': 'test_lastmodified', 'size': 'test_size'
        }
        self.dbm.add_record(**test_record)
        self.assertEqual(test_record, self.dbm.dict_list[0])
        self.dbm.flush_dict()


