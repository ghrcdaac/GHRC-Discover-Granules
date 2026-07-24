from task.dbm_cumulus import get_db_manager_cumulus
from task.dbm_postgresql import get_db_manager_psql


def get_db_manager(db_type, **kwargs):
    print(f'Creating {db_type} database manager...')

    if db_type == 'cumulus':
        dbm = get_db_manager_cumulus(**kwargs)
    else:
        dbm = get_db_manager_psql(**kwargs)

    return dbm


if __name__ == '__main__':
    pass
