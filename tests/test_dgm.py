
import unittest
from unittest.mock import MagicMock, patch

from task.dbm_base import DBManagerBase
from task.dbm_get import get_db_manager


class TestDGM(unittest.TestCase):
    def setUp(self) -> None:
        self.collection_id = 'fake_collection'
        self.url = 'url'

    @patch('task.dbm_get.get_db_manager_sqlite')
    def test_get_dbm_sqlite(self, mock_sqlite):
        dbm = get_db_manager(
            collection_id=self.collection_id, provider_url=self.url, db_type='sqlite', database=':memory:',
            batch_limit=5
        )
        mock_sqlite.assert_called_once()

    @patch('task.dbm_get.get_db_manager_psql')
    def test_get_dbm_postgresql(self, mock_psql):
        dbm = get_db_manager(
            collection_id=self.collection_id, provider_url=self.url, db_type='postgresql', database='pytest',
            batch_limit=5
        )
        mock_psql.assert_called_once()

    @patch('task.dbm_get.get_db_manager_cumulus')
    def test_get_dbm_cumulus(self, mock_cumulus):
        dbm = get_db_manager(
            collection_id=self.collection_id, provider_url=self.url, db_type='cumulus', database=MagicMock(),
            batch_limit=5
        )
        mock_cumulus.assert_called_once()

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
                super().read_batch()

        test_dbm = TestDBM()
        self.assertRaises(NotImplementedError, test_dbm.close_db)
        self.assertRaises(NotImplementedError, test_dbm.flush_dict)
        self.assertRaises(NotImplementedError, test_dbm.read_batch)
