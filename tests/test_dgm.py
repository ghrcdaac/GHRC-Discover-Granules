import contextlib
import os

import unittest
from tempfile import mkstemp

from task.dgm import initialize_db, db, Granule


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = f'{mkstemp()[1]}.db'


class TestDbInit(unittest.TestCase):
    """
    Tests Db Initialization
    """
    def tearDown(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(DB_PATH)
            os.remove(f'{DB_PATH}-shm')
            os.remove(f'{DB_PATH}-wal')

    def test_initialize_db(self):
        self.assertEqual(db.database, None)
        local_db = initialize_db(DB_PATH)
        self.assertEqual(local_db.database, DB_PATH)


class TestDGM(unittest.TestCase):
    """
    Tests DGM
    """
    def setUp(self) -> None:
        self.db = initialize_db(DB_PATH)
        self.model = Granule()

    def tearDown(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(DB_PATH)
            os.remove(f'{DB_PATH}-shm')
            os.remove(f'{DB_PATH}-wal')

    def test_db_select_all(self):
        discovered_granules = {
            "granule_a": {"ETag": "tag1_a", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified_a"},
            "granule_b": {"ETag": "tag1_b", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified_b"}
        }
        self.model.db_skip(discovered_granules)
        names = self.model.select_all(discovered_granules)
        self.assertEqual(len(names), 2)

    def test_db_error_exception(self):
        discovered_granules = {"granule_a": {"ETag": "tag1", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        with self.assertRaises(Exception) as context:
            self.model.db_error(discovered_granules)
            self.model.db_error(discovered_granules)
            self.assertTrue('A duplicate granule was found' in context.exception)

    def test_db_error_no_exception(self):
        discovered_granules_1 = {"granule_a": {"ETag": "tag1", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        discovered_granules_2 = {"granule_b": {"ETag": "tag2", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        num1 = self.model.db_error(discovered_granules_1)
        num2 = self.model.db_error(discovered_granules_2)
        self.assertTrue(num1 == len(discovered_granules_1))
        self.assertTrue(num2 == len(discovered_granules_2))

    def test_db_skip_no_update(self):
        discovered_granules = {"granule_a": {"ETag": "tag1", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        num1 = self.model.db_skip(discovered_granules)
        num2 = self.model.db_skip(discovered_granules)
        self.assertEqual(num1, 1)
        self.assertEqual(num2, 0)

    def test_db_skip_update_etag(self):
        discovered_granules_1 = {"granule_a": {"ETag": "tag1", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        discovered_granules_2 = {'granule_a': {"ETag": "tag1_a", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        num1 = self.model.db_skip(discovered_granules_1)
        self.assertEqual(num1, 1)
        num2 = self.model.db_skip(discovered_granules_2)
        self.assertEqual(num2, 1)

    def test_db_skip_update_modified(self):
        discovered_granules_1 = {"granule_a": {"ETag": "tag1", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        discovered_granules_2 = {'granule_a': {"ETag": "tag1_a", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        num1 = self.model.db_skip(discovered_granules_1)
        self.assertEqual(num1, 1)
        num2 = self.model.db_skip(discovered_granules_2)
        self.assertEqual(num2, 1)

    def test_db_skip_new_granule(self):
        discovered_granule_a = {"granule_a": {"ETag": "tag1_a", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified_a"}}
        discovered_granule_b = {"granule_b": {"ETag": "tag1_b", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified_b"}}
        num1 = self.model.db_skip(discovered_granule_a)
        num2 = self.model.db_skip(discovered_granule_b)
        self.assertEqual(num1, 1)
        self.assertEqual(num2, 1)

    def test_db_replace(self):
        discovered_granules = {"granule_a": {"ETag": "tag1a", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified"}}
        num1 = self.model.db_replace(discovered_granules)
        self.assertEqual(num1, 1)
        num1 = self.model.db_replace(discovered_granules)
        self.assertEqual(num1, 1)

    def test_db_delete_granules_by_name(self):
        discovered_granules = {"granule_a": {"ETag": "tag1_a", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified_a"},
                               "granule_b": {"ETag": "tag1_b", "GranuleId": "granule_id2", "CollectionId": "collection_id1", "Last-Modified": "modified_b"}}
        num1 = self.model.db_skip(discovered_granules)
        self.assertEqual(num1, 2)
        del_count = self.model.delete_granules_by_names([x for x in discovered_granules])
        self.assertEqual(del_count, 2)

    def test_db_insert_many(self):
        discovered_granules = {"granule_a": {"ETag": "tag1_a", "GranuleId": "granule_id1", "CollectionId": "collection_id1", "Last-Modified": "modified_a"},
                               "granule_b": {"ETag": "tag1_b", "GranuleId": "granule_id2", "CollectionId": "collection_id1", "Last-Modified": "modified_b"}}
        count = self.model._Granule__insert_many(discovered_granules)
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()
