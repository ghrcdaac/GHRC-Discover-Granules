from task.dgm_cumulus import DBManagerCumulus
from task.dgm_postgresql import DBManagerPostgresql
from task.dgm_sqlite import DBManagerSQLite


def get_db_manager(db_type, database=None, duplicate_handling='skip', transaction_size=100000, cumulus_filter=False):
    print(f'Creating {db_type} database manager and cumulus_filtering {cumulus_filter}')

    if db_type == 'cumulus':
        dbm = DBManagerCumulus(
            db_type=db_type, database=database, duplicate_handling=duplicate_handling, transaction_size=transaction_size
        )
    elif db_type == 'postgresql':
        dbm = DBManagerPostgresql(
            db_type=db_type, database=database, duplicate_handling=duplicate_handling,
            transaction_size=transaction_size, cumulus_filter=cumulus_filter
        )
    else:
        dbm = DBManagerSQLite(
            db_type=db_type, database=database, duplicate_handling=duplicate_handling,
            transaction_size=transaction_size, cumulus_filter=cumulus_filter
        )
    return dbm


if __name__ == '__main__':
    pass
