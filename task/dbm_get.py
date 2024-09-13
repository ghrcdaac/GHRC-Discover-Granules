from task.dbm_cumulus import get_db_manager_cumulus
from task.dbm_postgresql import get_db_manager_psql
from task.dbm_sqlite import get_db_manager_sqlite


def get_db_manager(db_type, **kwargs):
    print(f'Creating {db_type} database manager...')

    if db_type == 'cumulus':
        dbm = get_db_manager_cumulus(**kwargs)
    elif db_type == 'postgresql':
        dbm = get_db_manager_psql(**kwargs)
    else:
        dbm = get_db_manager_sqlite(**kwargs)

    return dbm


if __name__ == '__main__':
    pass
