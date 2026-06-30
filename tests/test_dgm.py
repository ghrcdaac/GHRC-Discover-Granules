import pytest

from task.dbm_base import DBManagerBase
from task.dbm_get import get_db_manager


class TestDBM(DBManagerBase):
    def __init__(self):
        super().__init__()

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


def test_get_dbm_postgresql(mocker):
    mock_psql = mocker.patch('task.dbm_get.get_db_manager_psql')
    dbm = get_db_manager(
            collection_id='fake_collection', provider_url='url', db_type='postgresql', database='pytest',
            batch_limit=5
    )
    mock_psql.assert_called()


def test_get_dbm_cumulus(mocker, mock_cumulus_dbm):
    mock_cumulus = mocker.patch('task.dbm_get.get_db_manager_cumulus')
    dbm = get_db_manager(
        collection_id='fake_collection', provider_url='url', db_type='cumulus', database=mock_cumulus_dbm,
        batch_limit=5
    )
    mock_cumulus.assert_called_once()


def test_abc_exceptions():
    test_dbm = TestDBM()
    with pytest.raises(NotImplementedError):
        test_dbm.close_db()
    with pytest.raises(NotImplementedError):
        test_dbm.flush_dict()
    with pytest.raises(NotImplementedError):
        test_dbm.read_batch()
