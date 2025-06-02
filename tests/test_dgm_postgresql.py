import time
import psycopg2
import pytest

from task.dbm_postgresql import get_db_manager_psql


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
        'provider_url': 'protocol://host/path/'
    }
    db = get_db_manager_psql(**db_args)
    return db

def test_discover_and_read_batch(postgresql_service):
    postgresql_service.add_record(f'protocol://host/path/gid_1', 'gid_1', 'test_id___1', 'fake_etag', 'fake_last_mod', 100)
    postgresql_service.flush_dict()
    assert postgresql_service.discovered_files_count == 1

    batch = postgresql_service.read_batch()
    assert len(batch) == 1
    print(batch)
