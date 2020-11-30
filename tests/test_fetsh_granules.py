# Test here
from task.main import DiscoverGranules
import unittest


class TestDiscoverGranules(unittest.TestCase):
    def test_get_file_link(self):
        tempList = DiscoverGranules.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/')
        assert len(tempList) == 61

    def test_get_file_link_wregex(self):
        tempList = DiscoverGranules.get_files_link_http('http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m04/', "\Bd3d")
        assert len(tempList) == 30

    def test_bad_url(self):
        DiscoverGranules.get_files_link_http('malformed url')


if __name__ == "__main__":
    unittest.main()
