from task.dbm_cumulus import get_db_manager_cumulus
from task.dbm_postgresql import get_db_manager_psql
from task.dbm_sqlite import get_db_manager_sqlite


def get_db_manager(db_type, database=None, duplicate_handling='skip', transaction_size=100000, cumulus_filter_dbm=None):
    print(f'Creating {db_type} database manager with cumulus_filtering {True if cumulus_filter_dbm else False}')

    if db_type == 'cumulus':
        dbm = get_db_manager_cumulus(
            database=database, duplicate_handling=duplicate_handling, transaction_size=transaction_size
        )
    elif db_type == 'postgresql':
        dbm = get_db_manager_psql(
            database=database, duplicate_handling=duplicate_handling, transaction_size=transaction_size,
            cumulus_filter=cumulus_filter_dbm
        )
    else:
        dbm = get_db_manager_sqlite(
            database=database, duplicate_handling=duplicate_handling, transaction_size=transaction_size,
            cumulus_filter=cumulus_filter_dbm
        )

    return dbm


if __name__ == '__main__':
    pass
