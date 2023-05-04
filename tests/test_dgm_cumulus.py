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
        os.environ['db_type'] = 'cumulus'
        from task.dgm import get_db_manager, DBManagerCumulus
        dbm = get_db_manager(db_type='cumulus')
        self.assertIsInstance(dbm, DBManagerCumulus)
