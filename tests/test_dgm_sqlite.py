import os
import unittest
from unittest.mock import patch


class TestDGMImports(unittest.TestCase):
    @patch('json.loads')
    @patch('boto3.client')
    def test_sqlite_config(self, mock_client, mock_json_load):
        mock_client.get_secret_value.return_value = {}
        mock_json_load.return_value = {}
        os.environ['db_type'] = 'sqlite'
        from task.dgm import get_db_manager, DBManager
        dbm = get_db_manager(db_type='sqlite')
        self.assertIsInstance(dbm, DBManager)


class TestDGMDBM(unittest.TestCase):
    def setUp(self) -> None:
        os.environ['db_type'] = ':memory:'
        from task.dgm import get_db_manager
        self.dbm = get_db_manager(db_type=':memory:')

    def tearDown(self) -> None:
        self.dbm.close_db()

    def test_dbm_postgresql(self):
        self.dbm.close_db()

    def test_dbm_postgresql_2(self):
        collection_id = 'sqlite_collection_id_1'
        provider_path = 'sqlite_name_1'
        batch_size = 1000
        test_record = {
            'name': 'sqlite_name_1', 'granule_id': 'sqlite_granule_id_1', 'collection_id': collection_id,
            'etag': 'etag', 'last_modified': 'test_lastmodified', 'size': 'test_size'
        }
        self.dbm.add_record(**test_record)
        self.assertEqual(test_record, self.dbm.dict_list[0])
        self.dbm.flush_dict()
        self.assertEqual(1, self.dbm.discovered_files_count)
        batch = self.dbm.read_batch(collection_id, provider_path, batch_size)
        self.assertEqual(1, len(batch))
        self.dbm.duplicate_handling = 'skip'
        batch = self.dbm.read_batch(collection_id, provider_path, batch_size)
        self.assertEqual(0, len(batch))





