import os

import unittest
from tempfile import mkstemp
from unittest.mock import MagicMock, patch

from task.dgm import DB, get_db_manager, DBManager, DBManagerBase

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = f'{mkstemp()[1]}.db'


def generate_test_dict(collection_id, granule_count=1, file_count=1, collection_count=1, new_etag='', new_last_mod='',
                       new_size=''):
    ret = {
        'total_collections': collection_count,
        'total_granules': collection_count * granule_count,
        'total_files': collection_count * granule_count * collection_count,
        'collection_granules': granule_count,
        'collection_files': file_count * granule_count
    }

    granule_dict = {}
    for x in range(collection_count):
        if x != 0:
            collection_id = f'collection_id_{x}'

        for y in range(granule_count):
            granule_id = f'granule_id_{y}'
            for z in range(file_count):
                granule_name = f'granule_name_{collection_id}_{granule_id}_{z}'
                etag = f'etag{new_etag}_{z}'
                last_mod = f'modified{new_last_mod}_{z}'
                size = new_size if new_size else 1
                granule_dict.update({
                    granule_name: {
                        'etag': etag, 'granule_id': granule_id,
                        'collection_id': collection_id,
                        'last_modified': last_mod, 'size': size}
                })

    ret.update({'granule_dict': granule_dict})
    return ret


def generate_db_dict(input_dict):
    ret = []
    for k, v in input_dict.items():
        td = {'name': k}
        for k2, v2 in v.items():
            td.update({k2: v2})
        ret.append(td)

    return ret


class TestDGM(unittest.TestCase):
    """
    Tests DGM
    """

    def setUp(self) -> None:
        self.dbm = get_db_manager(db_type=':memory:')
        self.granule = self.dbm.model

    @patch('task.dgm.DBManagerCumulus')
    def test_get_db_manager_class_cumulus(self, dgm_mock):
        _ = get_db_manager(db_type='cumulus')
        self.assertTrue(dgm_mock.called)

    def test_get_db_manager_class_postgresql(self):
        DB.init = MagicMock()
        DB.create_tables = MagicMock()
        DB.close = MagicMock()
        dbm = get_db_manager(db_type='postgresql')
        self.assertTrue(isinstance(dbm, DBManager))

    def test_db_error_exception(self):
        test_dict = generate_test_dict(collection_id='test_db_error_exception')
        granule_dict = generate_db_dict(test_dict.get('granule_dict'))
        with self.assertRaises(Exception) as context:
            self.granule.db_error(granule_dict)
            self.granule.db_error(granule_dict)
            self.assertTrue('A duplicate granule was found' in context.exception)

    def test_db_error_no_exception(self):
        for collection_id in ['test_db_error_no_exception_1', 'test_db_error_no_exception_2']:
            test_dict = generate_test_dict(collection_id=collection_id)
            granule_dict = generate_db_dict(test_dict.get('granule_dict'))
            _ = self.granule.db_error(granule_dict)

    def test_db_skip_no_update(self):
        test_dict = generate_test_dict(collection_id='test_db_skip_no_update')
        granule_dict = generate_db_dict(test_dict.get('granule_dict'))
        for x in [test_dict.get('total_files'), 0]:
            num1 = self.granule.db_skip(granule_dict)
            self.assertEqual(num1, x)

    def test_db_skip_update_etag(self):
        print('etag start')
        collection_id = 'test_db_skip_update_etag'
        for etag in ['', '_new']:
            test_dict = generate_test_dict(collection_id=collection_id, new_etag=etag)
            granule_dict = generate_db_dict(test_dict.get('granule_dict'))
            num1 = self.granule.db_skip(granule_dict)
            self.assertEqual(num1, test_dict.get('total_files'))
        print('etag end')

    def test_db_skip_update_modified(self):
        collection_id = 'test_db_skip_update_modified'
        for last_mod in ['', '_new']:
            test_dict = generate_test_dict(collection_id=collection_id, new_last_mod=last_mod)
            granule_dict = generate_db_dict(test_dict.get('granule_dict'))
            num1 = self.granule.db_skip(granule_dict)
            self.assertEqual(num1, test_dict.get('total_files'))

    def test_db_skip_new_granule(self):
        expected_count = 1
        for x in [1, 2]:
            test_dict = generate_test_dict(collection_id='test_db_skip_new_granule', granule_count=x)
            granule_dict = generate_db_dict(test_dict.get('granule_dict'))
            num1 = self.granule.db_skip(granule_dict)
            self.assertEqual(expected_count, num1)

    def test_db_replace(self):
        test_dict = generate_test_dict(collection_id='test_db_replace')
        granule_dict = generate_db_dict(test_dict.get('granule_dict'))

        for _ in range(2):
            num1 = self.granule.db_replace(granule_dict)
            self.assertEqual(num1, test_dict.get('total_files'))

    def test_db_insert_many(self):
        test_dict = generate_test_dict(collection_id='test_db_skip_new_granule', granule_count=2, file_count=2,
                                       collection_count=2)
        granule_dict = generate_db_dict(test_dict.get('granule_dict'))
        # pylint: disable=W0212
        count = self.granule._Granule__insert_many(granule_dict, **{'conflict_resolution': {'action': 'ignore'}})

        self.assertEqual(test_dict.get('total_files'), count)

    def test_db_fetch_batch(self):
        collection_id = 'test_db_fetch_batch'
        discovered_granules = generate_test_dict(collection_id=collection_id)
        granule_dict = generate_db_dict(discovered_granules.get('granule_dict'))
        self.granule.db_skip(granule_dict)
        batch = self.granule.read_batch(collection_id=collection_id, provider_path='', batch_size=1)

        self.assertEqual(1, len(batch))

    def test_db_count_records_discovered_files(self):
        collection_id = 'test_db_count_records_discovered_files'
        provider_path = collection_id
        kwargs = {
            'granule_count': 2, 'file_count': 2, 'collection_count': 2, 'collection_id': collection_id
        }

        test_dict = generate_test_dict(**kwargs)
        inserted_files = self.granule.db_skip(generate_db_dict(test_dict.get('granule_dict')))
        self.assertEqual(inserted_files, test_dict.get('total_files'))
        counted_files = self.granule.count_records(collection_id, provider_path)

        self.assertEqual(test_dict.get('collection_files'), counted_files)

    def test_db_count_records_discovered_granules(self):
        collection_id = 'test_db_count_records_discovered_granules'
        provider_path = collection_id
        kwargs = {
            'granule_count': 2, 'file_count': 2, 'collection_count': 2, 'collection_id': collection_id
        }

        test_dict = generate_test_dict(**kwargs)
        inserted_files = self.granule.db_skip(generate_db_dict(test_dict.get('granule_dict')))
        self.assertEqual(inserted_files, test_dict.get('total_files'))
        counted_files = self.granule.count_records(collection_id, provider_path, count_type='granules')
        self.assertEqual(test_dict.get('collection_granules'), counted_files)

    def test_db_count_records_queued_files(self):
        collection_id = 'test_db_count_records_queued_files'
        provider_path = collection_id
        test_dict = generate_test_dict(collection_id=collection_id)
        granule_dict = generate_db_dict(test_dict.get('granule_dict'))
        inserted_files = self.granule.db_skip(granule_dict)
        self.assertEqual(inserted_files, test_dict.get('total_files'))

        counted_files = self.granule.count_records(collection_id, provider_path, status='queued')
        self.assertEqual(0, counted_files)

        _ = self.granule.read_batch(collection_id, provider_path)
        counted_files = self.granule.count_records(collection_id, provider_path, status='queued')
        self.assertEqual(1, counted_files)

    def test_db_count_records_queued_granules(self):
        collection_id = 'test_db_count_records_queued_granules'
        provider_path = collection_id
        test_dict = generate_test_dict(collection_id=collection_id)
        granule_dict = generate_db_dict(test_dict.get('granule_dict'))
        inserted_files = self.granule.db_skip(granule_dict)
        self.assertEqual(inserted_files, test_dict.get('total_files'))

        counted_granules = self.granule.count_records(
            collection_id, provider_path, status='queued', count_type='granules'
        )
        self.assertEqual(0, counted_granules)

        _ = self.granule.read_batch(collection_id, provider_path)
        counted_granules = self.granule.count_records(
            collection_id, provider_path, status='queued', count_type='granules'
        )
        self.assertEqual(1, counted_granules)

    def test_data_generator(self):
        collection_id = 'test_data_generator'
        test_dict = generate_test_dict(collection_id=collection_id)
        granule_dict = test_dict.get('granule_dict')
        tuple_generator = self.granule.data_generator(granule_dict)
        tuple_lst = []
        for k, v in granule_dict.items():
            tuple_lst.append((k, v['etag'], v['granule_id'], v['collection_id'], 'discovered', v['last_modified'], v['size']))

        generated_tuple_lst = list(tuple_generator)
        self.assertEqual(generated_tuple_lst, tuple_lst)

    def test_abc_exceptions(self):
        class TestDBM(DBManagerBase):
            def __init__(self):
                super().__init__()

            def close_db(self):
                super().close_db()
                pass

            def add_record(self, name=None, granule_id=None, collection_id=None, etag=None, last_modified=None,
                           size=None):
                super().add_record(name=name, granule_id=granule_id, collection_id=collection_id, etag=etag,
                                   last_modified=last_modified, size=size)
                pass

            def flush_dict(self):
                super().flush_dict()
                pass

            def read_batch(self, collection_id=None, provider_path=None, batch_size=None):
                super().read_batch(collection_id=collection_id, provider_path=provider_path, batch_size=batch_size)
                pass

        test_dbm = TestDBM()
        self.assertRaises(NotImplementedError, test_dbm.close_db)
        self.assertRaises(NotImplementedError, test_dbm.add_record)
        self.assertRaises(NotImplementedError, test_dbm.flush_dict)
        self.assertRaises(NotImplementedError, test_dbm.read_batch)


if __name__ == "__main__":
    unittest.main()
