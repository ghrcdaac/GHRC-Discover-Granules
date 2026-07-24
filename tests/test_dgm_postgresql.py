import pytest

from task.dbm_get import get_db_manager


@pytest.fixture(scope="session")
def test_dict_factory():
    gid_idx = 0

    def gen_test_dict(provider_url, collection_id, granule_count=1, file_count=1, etag='',
                      last_mod='', size=1):
        test_dict = {
            'total_collections': 1,
            'total_granules': granule_count,
            'total_files': granule_count,
            'collection_granules': granule_count,
            'collection_files': file_count * granule_count
        }

        nonlocal gid_idx
        granule_list_dict = []
        for i in range(granule_count):
            gid_idx += 1
            granule_id = f'granule_id_{gid_idx}'
            for j in range(file_count):
                granule_list_dict.append({
                    'name': f'{provider_url}_granule_name_{collection_id}_{granule_id}_{j}',
                    'etag': etag if etag else f'etag_{gid_idx}',
                    'granule_id': granule_id,
                    'collection_id': collection_id,
                    'last_modified': last_mod if last_mod else f'modified_{gid_idx}_{j}',
                    'size': size
                })

        test_dict.update({'granule_list_dict': granule_list_dict})
        return test_dict

    return gen_test_dict


def test_discover_and_read_batch(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id
    )
    record = test_dict.get('granule_list_dict')[0]
    dbm.add_record(**record)
    dbm.flush_dict()
    assert dbm.discovered_files_count == 1

    batch = dbm.read_batch()
    assert len(batch) == 1
    print(batch)


def test_psql_skip_no_update(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    results = []
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id
    )
    dbm.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            dbm.add_record(**record)
            dbm.write_batch()
            results.append(dbm.read_batch())

    assert len(results[0]) == 1
    assert len(results[1]) == 0


def test_psql_skip_update_etag(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        etag='test_etag'
    )
    dbm.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            dbm.add_record(**record)
            assert dbm.write_batch() == 1
            record['etag'] += f'_{i}'

    batch = dbm.read_batch()
    assert len(batch) == 1


def test_psql_skip_update_modified(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        last_mod='test_mod'
    )
    dbm.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            dbm.add_record(**record)
            assert dbm.write_batch() == 1
            record['last_modified'] += f'_{i}'

    batch = dbm.read_batch()
    assert len(batch) == 1


def test_psql_skip_update_size(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        size=8
    )
    dbm.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            dbm.add_record(**record)
            assert dbm.write_batch() == 1
            record['size'] *= 2

    batch = dbm.read_batch()
    assert len(batch) == 1


def test_psql_skip_update_check_value(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        etag='test_etag__00', last_mod='2022-02-22 22:22:22+00:00', size=8
    )
    dbm.file_count = 1

    orig_record = test_dict['granule_list_dict'][0]
    dbm.add_record(**orig_record)
    dbm.write_batch()
    orig_row = dbm.read_batch()[0]
    print(f'Original row: {orig_row}')

    assert orig_record['etag'] == orig_row['etag']
    assert orig_record['size'] == orig_row['size']
    assert orig_record['last_modified'] == orig_row['last_modified']

    updated_record = orig_record.copy()
    updated_record['size'] = 16
    updated_record['last_modified'] = '2023-03-33 33:33:33+00:00'
    dbm.add_record(**updated_record)
    dbm.write_batch()
    updated_row = dbm.read_batch()[0]
    print(f'Updated row: {updated_row}')

    assert updated_record['etag'] == updated_row['etag']
    assert updated_record['size'] == updated_row['size']
    assert updated_record['last_modified'] == updated_row['last_modified']
    assert updated_row['discovered_date'] >= orig_row['discovered_date']


def test_psql_skip_new_granule(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        granule_count=2
    )
    dbm.file_count = 1

    for record in test_dict.get('granule_list_dict'):
        dbm.add_record(**record)
        assert dbm.write_batch() == 1

    batch = dbm.read_batch()
    assert len(batch) == 2


def test_db_replace(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    dbm.duplicate_handling = 'replace'
    total = 0
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        etag='a', last_mod=str(0)
    )
    dbm.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            dbm.add_record(**record)
            total += dbm.write_batch()
            batch = dbm.read_batch()
            assert len(batch) == 1
    assert total == 2


def test_ignore_discovered(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        granule_count=4
    )
    dbm.file_count = 1

    for record in test_dict.get('granule_list_dict'):
        dbm.add_record(**record)
        dbm.write_batch()

    dbm.ignore_discovered()
    ignored_count = dbm.model_class.select(dbm.model_class.name).where(
        dbm.model_class.status == 'ignored').count()
    assert ignored_count == 4


def test_add_for_update():
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    base_query = dbm.model_class.select()
    for_update_query = dbm.add_for_update(base_query)
    print(for_update_query)
    assert 'FOR UPDATE' in str(for_update_query)


def test_psql_too_many_files(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        granule_count=dbm.batch_limit + 10
    )
    dbm.file_count = 1

    for record in test_dict.get('granule_list_dict'):
        dbm.add_record(**record)

    dbm.write_batch()
    full_batch = dbm.read_batch()
    assert len(full_batch) == dbm.batch_limit
    rem_batch = dbm.read_batch()
    assert len(rem_batch) == 10


def test_psql_skip_complete_multifile_granule(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        file_count=3
    )
    dbm.file_count = 3

    for record in test_dict.get('granule_list_dict'):
        dbm.add_record(**record)

    dbm.write_batch()
    batch = dbm.read_batch()
    assert len(batch) == 3


def test_psql_skip_incomplete_multifile_granule(test_dict_factory):
    dbm = get_db_manager(db_type='postgresql', database='pytest')
    test_dict = test_dict_factory(
        provider_url=dbm.provider_full_url, collection_id=dbm.collection_id,
        file_count=3
    )
    test_dict.get('granule_list_dict').pop(-1)
    dbm.file_count = 3

    for record in test_dict.get('granule_list_dict'):
        dbm.add_record(**record)

    dbm.write_batch()
    batch = dbm.read_batch()
    assert len(batch) == 0
