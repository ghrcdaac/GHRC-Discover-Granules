import time
import psycopg2
import pytest

from task.dbm_postgresql import get_db_manager_psql


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


def is_db_ready(docker_ip, port):
    try:
        with psycopg2.connect(dbname='pytest', user='pytest', password='pytest', host=docker_ip, port=port) as db:
            pass
        return True
    except psycopg2.OperationalError:
        return False


@pytest.fixture(scope="session")
def postgresql_service(docker_ip, docker_services):
    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("psql_db", 5432)
    docker_services.wait_until_responsive(
        timeout=60.0, pause=0.1, check=lambda: is_db_ready(docker_ip, port)
    )
    db_args = {
        'database': 'pytest',
        'user': 'pytest',
        'password': 'pytest',
        'host': docker_ip,
        'port': port,
        'collection_id': 'test_id___1',
        'provider_url': 'protocol://host/path/',
        'batch_limit': 100
    }
    db = get_db_manager_psql(**db_args)
    return db


def test_discover_and_read_batch(postgresql_service, test_dict_factory):
    postgresql_service.add_record(f'protocol://host/path/gid_1', 'gid_1', 'test_id___1', 'fake_etag', 'fake_last_mod', 100)
    postgresql_service.flush_dict()
    assert postgresql_service.discovered_files_count == 1

    batch = postgresql_service.read_batch()
    assert len(batch) == 1
    print(batch)


def test_psql_skip_no_update(postgresql_service, test_dict_factory):
    results = []
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id
    )
    postgresql_service.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            postgresql_service.add_record(**record)
            postgresql_service.write_batch()
            results.append(postgresql_service.read_batch())

    assert len(results[0]) == 1
    assert len(results[1]) == 0


def test_psql_skip_update_etag(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        etag='test_etag'
    )
    postgresql_service.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            postgresql_service.add_record(**record)
            assert postgresql_service.write_batch() == 1
            record['etag'] += f'_{i}'

    batch = postgresql_service.read_batch()
    assert len(batch) == 1


def test_psql_skip_update_modified(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        last_mod='test_mod'
    )
    postgresql_service.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            postgresql_service.add_record(**record)
            assert postgresql_service.write_batch() == 1
            record['last_modified'] += f'_{i}'

    batch = postgresql_service.read_batch()
    assert len(batch) == 1


def test_psql_skip_update_size(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        size=8
    )
    postgresql_service.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            postgresql_service.add_record(**record)
            assert postgresql_service.write_batch() == 1
            record['size'] *= 2

    batch = postgresql_service.read_batch()
    assert len(batch) == 1

def test_psql_skip_update_check_value(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        etag='test_etag__00', last_mod='2022-02-22 22:22:22+00:00', size=8
    )
    postgresql_service.file_count = 1

    orig_record = test_dict['granule_list_dict'][0]
    postgresql_service.add_record(**orig_record)
    postgresql_service.write_batch()
    orig_row = postgresql_service.read_batch()[0]
    print(f'Original row: {orig_row}')

    assert orig_record['etag'] == orig_row['etag']
    assert orig_record['size'] == orig_row['size']
    assert orig_record['last_modified'] == orig_row['last_modified']

    updated_record = orig_record.copy()
    updated_record['size'] = 16
    updated_record['last_modified'] = '2023-03-33 33:33:33+00:00'
    postgresql_service.add_record(**updated_record)
    postgresql_service.write_batch()
    updated_row = postgresql_service.read_batch()[0]
    print(f'Updated row: {updated_row}')

    assert updated_record['etag'] == updated_row['etag']
    assert updated_record['size'] == updated_row['size']
    assert updated_record['last_modified'] == updated_row['last_modified']
    assert updated_row['discovered_date'] >= orig_row['discovered_date']


def test_psql_skip_new_granule(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        granule_count=2
    )
    postgresql_service.file_count = 1

    for record in test_dict.get('granule_list_dict'):
        postgresql_service.add_record(**record)
        assert postgresql_service.write_batch() == 1

    batch = postgresql_service.read_batch()
    assert len(batch) == 2


def test_db_replace(postgresql_service, test_dict_factory):
    postgresql_service.duplicate_handling = 'replace'
    total = 0
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        etag='a', last_mod=str(0)
    )
    postgresql_service.file_count = 1

    for i in range(2):
        for record in test_dict.get('granule_list_dict'):
            postgresql_service.add_record(**record)
            total += postgresql_service.write_batch()
            batch = postgresql_service.read_batch()
            assert len(batch) == 1
    assert total == 2


def test_ignore_discovered(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        granule_count=4
    )
    postgresql_service.file_count = 1

    for record in test_dict.get('granule_list_dict'):
        postgresql_service.add_record(**record)
        postgresql_service.write_batch()

    postgresql_service.ignore_discovered()
    ignored_count = postgresql_service.model_class.select(postgresql_service.model_class.name).where(
        postgresql_service.model_class.status == 'ignored').count()
    assert ignored_count == 4


def test_add_for_update(postgresql_service):
    base_query = postgresql_service.model_class.select()
    for_update_query = postgresql_service.add_for_update(base_query)
    print(for_update_query)
    assert 'FOR UPDATE' in str(for_update_query)


def test_psql_too_many_files(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        granule_count=postgresql_service.batch_limit + 10
    )
    postgresql_service.file_count = 1

    for record in test_dict.get('granule_list_dict'):
        postgresql_service.add_record(**record)

    postgresql_service.write_batch()
    full_batch = postgresql_service.read_batch()
    assert len(full_batch) == postgresql_service.batch_limit
    rem_batch = postgresql_service.read_batch()
    assert len(rem_batch) == 10


def test_psql_skip_complete_multifile_granule(postgresql_service, test_dict_factory):
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        file_count=3
    )
    postgresql_service.file_count = 3

    for record in test_dict.get('granule_list_dict'):
        postgresql_service.add_record(**record)

    postgresql_service.write_batch()
    batch = postgresql_service.read_batch()
    assert len(batch) == 3


def test_psql_skip_incomplete_multifile_granule(postgresql_service, test_dict_factory):
    postgresql_service.file_count = 3
    test_dict = test_dict_factory(
        provider_url=postgresql_service.provider_full_url, collection_id=postgresql_service.collection_id,
        file_count=3
    )
    test_dict.get('granule_list_dict').pop(-1)
    postgresql_service.file_count = 3

    for record in test_dict.get('granule_list_dict'):
        postgresql_service.add_record(**record)

    postgresql_service.write_batch()
    batch = postgresql_service.read_batch()
    assert len(batch) == 0
