# Test here
from task.main import DiscoverGranules
import unittest

# You are trying to test something dynamic 
# Imagine if the provider add a new file your return count will be 62
class TestDiscoverGranules(unittest.TestCase):
    def test_get_file_link(self):
        tempList = DiscoverGranules.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/')
        self.assertEqual(len(list(tempList)), 61)

    def test_get_file_link_wregex(self):
        tempList = DiscoverGranules.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/', "^f16_\\d{6}11v7\\.gz$")
        self.assertEqual(len(list(tempList)), 1)

    def test_bad_url(self):
        tempList = DiscoverGranules.get_files_link_http('malformed url')
        self.assertEqual(len(list(tempList)), 0)


if __name__ == "__main__":
    unittest.main()
