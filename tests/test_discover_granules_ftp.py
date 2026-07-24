import os

from task.discover_granules_ftp import DiscoverGranulesFTP


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def test_discover_granules_ftp_list_output(mocker):
    dg = DiscoverGranulesFTP({}, None)
    with open(os.path.join(THIS_DIR, 'ftp_list_output.txt'), 'r+', encoding='utf-8') as sample_output:
        output = sample_output.read()
    mock_add_record = mocker.patch.object(dg.dbm, 'add_record')
    directory_list = []
    dg.process_ftp_list_output(output, directory_list)

    assert mock_add_record.call_count == 3
    assert len(directory_list) == 3