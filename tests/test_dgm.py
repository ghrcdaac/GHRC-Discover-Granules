# Test here
import os
from task.dgm import initialize_db, db
import unittest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = '/tmp/discover_granules_test_collection.db'


class TestDbInit(unittest.TestCase):
    def test_initialize_db(self):
        self.assertEqual(db.database, None)
        initialize_db(DB_PATH)
        self.assertEqual(db.database, DB_PATH)


class TestDGM(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = DB_PATH
        self.dgm = initialize_db(self.db_path)

    def tearDown(self) -> None:
        os.remove(DB_PATH)
        os.remove(f'{DB_PATH}-shm')
        os.remove(f'{DB_PATH}-wal')

    def test_db_select_all(self):
        discovered_granules = {"granule_a": {"ETag": "tag1_a", "Last-Modified": "modified_a"},
                               "granule_b": {"ETag": "tag1_b", "Last-Modified": "modified_b"}}
        self.dgm.db_skip(discovered_granules)
        names = self.dgm.select_all(discovered_granules)
        self.assertEqual(len(names), 2)

    def test_db_error_exception(self):
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        with self.assertRaises(Exception) as context:
            self.dgm.db_error(discovered_granules)
            self.dgm.db_error(discovered_granules)
            self.assertTrue('A duplicate granule was found' in context.exception)

    def test_db_error_no_exception(self):
        discovered_granules_1 = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        discovered_granules_2 = {"granule_b": {"ETag": "tag2", "Last-Modified": "modified"}}
        n1 = self.dgm.db_error(discovered_granules_1)
        n2 = self.dgm.db_error(discovered_granules_2)
        self.assertTrue(n1 == len(discovered_granules_1))
        self.assertTrue(n2 == len(discovered_granules_2))

    def test_db_skip_no_update(self):
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        n1 = self.dgm.db_skip(discovered_granules)
        n2 = self.dgm.db_skip(discovered_granules)
        self.assertEqual(n1, 1)
        self.assertEqual(n2, 0)

    def test_db_skip_update_etag(self):
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        n1 = self.dgm.db_skip(discovered_granules)
        self.assertEqual(n1, 1)
        discovered_granules.update({'granule_a': {"ETag": "tag1_a", "Last-Modified": "modified"}})
        n2 = self.dgm.db_skip(discovered_granules)
        self.assertEqual(n2, 1)

    def test_db_skip_update_modified(self):
        discovered_granules = {"granule_a": {"ETag": "tag1", "Last-Modified": "modified"}}
        n1 = self.dgm.db_skip(discovered_granules)
        self.assertEqual(n1, 1)
        discovered_granules.update({'granule_a': {"ETag": "tag1", "Last-Modified": "modified_a"}})
        n2 = self.dgm.db_skip(discovered_granules)
        self.assertEqual(n2, 1)

    def test_db_skip_new_granule(self):
        discovered_granule_a = {"granule_a": {"ETag": "tag1_a", "Last-Modified": "modified_a"}}
        n1 = self.dgm.db_skip(discovered_granule_a)
        discovered_granule_b = {"granule_b": {"ETag": "tag1_b", "Last-Modified": "modified_b"}}
        n2 = self.dgm.db_skip(discovered_granule_b)
        self.assertEqual(n1, 1)
        self.assertEqual(n2, 1)

    def test_db_replace(self):
        discovered_granules = {"granule_a": {"ETag": "tag1a", "Last-Modified": "modified"}}
        n1 = self.dgm.db_replace(discovered_granules)
        self.assertEqual(n1, 1)
        n1 = self.dgm.db_replace(discovered_granules)
        self.assertEqual(n1, 1)

    def test_db_delete_granules_by_name(self):
        discovered_granules = {"granule_a": {"ETag": "tag1_a", "Last-Modified": "modified_a"},
                               "granule_b": {"ETag": "tag1_b", "Last-Modified": "modified_b"}}
        n1 = self.dgm.db_skip(discovered_granules)
        self.assertEqual(n1, 2)
        del_count = self.dgm.delete_granules_by_names([x for x in discovered_granules])
        self.assertEqual(del_count, 2)

    def test_db_insert_many(self):
        discovered_granules = {"granule_a": {"ETag": "tag1_a", "Last-Modified": "modified_a"},
                               "granule_b": {"ETag": "tag1_b", "Last-Modified": "modified_b"}}
        count = self.dgm._Granule__insert_many(discovered_granules)
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()
