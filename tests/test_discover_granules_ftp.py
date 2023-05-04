import os
import re
import unittest
from unittest.mock import patch, MagicMock

from task.discover_granules_ftp import DiscoverGranulesFTP

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDiscoverGranules(unittest.TestCase):
    def setUp(self) -> None:
        event = MagicMock()
        self.dg_client = DiscoverGranulesFTP(event)

    @patch.object(re, 'search')
    def test_process_ftp_list_output(self, re_mock):
        with open(os.path.join(THIS_DIR, 'ftp_list_output.txt'), 'r+', encoding='utf-8') as sample_output:
            output = sample_output.read()
        self.dg_client.dbm.add_record = MagicMock()
        directory_list = []
        self.dg_client.process_ftp_list_output(output, directory_list)
        self.assertEqual(3, self.dg_client.dbm.add_record.call_count)
        self.assertEqual(3, len(directory_list))
