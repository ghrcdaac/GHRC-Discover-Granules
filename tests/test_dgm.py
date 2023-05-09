
import unittest
from unittest.mock import MagicMock

from task.dbm_base import DBManagerBase
from task.dgm import get_db_manager
from task.dgm_cumulus import DBManagerCumulus
from task.dgm_postgresql import DBManagerPostgresql
from task.dgm_sqlite import DBManagerSQLite


class TestDGM(unittest.TestCase):
    def test_get_dbm_sqlite(self):
        dbm = get_db_manager(db_type='sqlite', database=':memory:')
        self.assertTrue(isinstance(dbm, DBManagerSQLite))

    def test_get_dbm_postgresql(self):
        dbm = get_db_manager(db_type='postgresql', database=MagicMock())
        self.assertTrue(isinstance(dbm, DBManagerPostgresql))

    def test_get_dbm_cumulus(self):
        dbm = get_db_manager(db_type='cumulus', database=MagicMock())
        self.assertTrue(isinstance(dbm, DBManagerCumulus))

    def test_abc_exceptions(self):
        class TestDBM(DBManagerBase):
            def __init__(self):
                super().__init__(db_type='sqlite', database=':memory:')

            def close_db(self):
                super().close_db()

            def add_record(self, name=None, granule_id=None, collection_id=None, etag=None, last_modified=None,
                           size=None):
                super().add_record(name=name, granule_id=granule_id, collection_id=collection_id, etag=etag,
                                   last_modified=last_modified, size=size)

            def flush_dict(self):
                super().flush_dict()

            def read_batch(self, collection_id=None, provider_path=None, batch_size=None):
                super().read_batch(collection_id=collection_id, provider_path=provider_path, batch_size=batch_size)

        test_dbm = TestDBM()
        self.assertRaises(NotImplementedError, test_dbm.close_db)
        self.assertRaises(NotImplementedError, test_dbm.flush_dict)
        self.assertRaises(NotImplementedError, test_dbm.read_batch)





