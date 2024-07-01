import unittest
from unittest.mock import MagicMock

from task.dbm_postgresql import DBManagerPSQL, GranulePSQL


class TestDBMPostgresql(unittest.TestCase):
    def setUp(self) -> None:
        kwargs = {
            'collection_id': 'fake_collection',
            'provider_url': 'protocol://fake_host/full/url',
            'model_class': GranulePSQL(),
            'database': MagicMock(),
            'auto_batching': True,
            'batch_limit': 100,
            'transaction_size': 100,
            'duplicate_handling': 'skip',
            'cumulus_filter': MagicMock(),
            'file_count': 1
        }
        self.dbm = DBManagerPSQL(**kwargs)

    def test_db_replace(self):
        self.dbm.insert_many = MagicMock()
        self.dbm.db_replace()
        self.assertTrue(self.dbm.insert_many.called)

    def test_for_update(self):
        query = self.dbm.add_for_update(self.dbm.model_class.select())
        self.assertIsNot(str(query).find('FOR UPDATE'), -1)
