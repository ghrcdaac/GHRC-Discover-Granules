import unittest

from task.dbm_get import get_db_manager


def generate_test_dict(provider_url, collection_id, granule_count=1, file_count=1, collection_count=1, new_etag='',
                       new_last_mod='', new_size=''):
    ret = {
        'total_collections': collection_count,
        'total_granules': collection_count * granule_count,
        'total_files': collection_count * granule_count * collection_count,
        'collection_granules': granule_count,
        'collection_files': file_count * granule_count
    }

    granule_list_dict = []
    for x in range(collection_count):
        if x != 0:
            collection_id = f'collection_id_{x}'

        for y in range(granule_count):
            granule_id = f'granule_id_{y}'
            for z in range(file_count):
                granule_name = f'{provider_url}_granule_name_{collection_id}_{granule_id}_{z}'
                etag = f'etag{new_etag}_{z}'
                last_mod = f'modified{new_last_mod}_{z}'
                size = new_size if new_size else 1
                granule_list_dict.append({
                    'name': granule_name,
                    'etag': etag, 'granule_id': granule_id,
                    'collection_id': collection_id,
                    'last_modified': last_mod, 'size': size
                })

    ret.update({'granule_list_dict': granule_list_dict})
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
        self.collection_id = 'test'
        self.provider_full_url = 'some://fake/full/url'
        self.dbm = get_db_manager(
            db_type='sqlite', database=':memory:', collection_id=self.collection_id,
            provider_url=self.provider_full_url, batch_limit=1000, duplicate_handling='skip'
        )

    def tearDown(self) -> None:
        self.dbm.close_db()

    def test_db_error_exception(self):
        test_dict = generate_test_dict(provider_url=self.provider_full_url, collection_id=self.collection_id)
        with self.assertRaises(Exception) as context:
            for x in range(2):
                for record in test_dict.get('granule_list_dict'):
                    self.dbm.add_record(**record)
                    self.dbm.write_batch()
                    self.dbm.db_error()

            self.assertTrue('A duplicate granule was found' in context.exception)

    def test_db_error_no_exception(self):
        test_dict = generate_test_dict(provider_url=self.provider_full_url, collection_id=self.collection_id)
        for x in range(2):
            for record in test_dict.get('granule_list_dict'):
                self.dbm.add_record(**record)
                self.dbm.write_batch()
                self.dbm.db_error()

    def test_db_skip_no_update(self):
        results = []
        test_dict = generate_test_dict(provider_url=self.provider_full_url, collection_id=self.collection_id)
        for x in range(2):
            for record in test_dict.get('granule_list_dict'):
                self.dbm.add_record(**record)
                self.dbm.write_batch()
                results.append(self.dbm.read_batch())

        self.assertEqual(1, len(results[0]))
        self.assertEqual(0, len(results[1]))

    def test_db_skip_update_etag(self):
        for x in range(2):
            test_dict = generate_test_dict(
                provider_url=self.provider_full_url, collection_id=self.collection_id, new_etag=str(x)
            )
            for record in test_dict.get('granule_list_dict'):
                self.dbm.add_record(**record)
                self.assertEqual(1, self.dbm.write_batch())

    def test_db_skip_update_modified(self):
        for x in range(2):
            test_dict = generate_test_dict(
                provider_url=self.provider_full_url, collection_id=self.collection_id, new_last_mod=str(x)
            )
            for record in test_dict.get('granule_list_dict'):
                self.dbm.add_record(**record)
                self.assertEqual(1, self.dbm.write_batch())

    def test_db_skip_new_granule(self):
        test_dict = generate_test_dict(
            provider_url=self.provider_full_url, collection_id=self.collection_id, granule_count=2
        )
        for record in test_dict.get('granule_list_dict'):
            self.dbm.add_record(**record)
            self.assertEqual(1, self.dbm.write_batch())

    def test_db_replace(self):
        self.dbm.duplicate_handling = 'replace'
        total = 0
        test_dict = generate_test_dict(
            provider_url=self.provider_full_url, collection_id=self.collection_id, new_last_mod=str(0)
        )
        for x in range(2):
            for record in test_dict.get('granule_list_dict'):
                self.dbm.add_record(**record)
                total += self.dbm.write_batch()
                batch = self.dbm.read_batch()
                self.assertEqual(1, len(batch))

        self.assertEqual(2, total)

    def test_dbm_sqlite_close(self):
        self.dbm.close_db()

    def test_dbm_full_test(self):
        granule_count = 2
        file_count = 2
        test_dict = generate_test_dict(
            provider_url=self.provider_full_url, collection_id=self.collection_id, granule_count=granule_count,
            file_count=file_count
        )
        for record in test_dict.get('granule_list_dict'):
            self.dbm.add_record(**record)
            self.assertEqual(record, self.dbm.list_dict[-1])

        self.assertEqual(4, self.dbm.flush_dict())

        batch = self.dbm.read_batch()
        self.assertEqual(granule_count * file_count, len(batch))

    def test_for_update(self):
        query = self.dbm.add_for_update(self.dbm.model_class.select())
        self.assertIs(str(query).find('FOR UPDATE'), -1)


if __name__ == "__main__":
    unittest.main()
