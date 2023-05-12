from task.dgm_cumulus import DBManagerCumulus
from task.dgm_postgresql import DBManagerPostgresql
from task.dgm_sqlite import DBManagerSQLite


def get_db_manager(db_type, database=None, duplicate_handling='skip', transaction_size=100000, cumulus_filter_dbm=None):
    print(f'Creating {db_type} database manager with cumulus_filtering {True if cumulus_filter_dbm else False}')

    if db_type == 'cumulus':
        dbm = DBManagerCumulus(
            db_type=db_type, database=database, duplicate_handling=duplicate_handling, transaction_size=transaction_size
        )
    elif db_type == 'postgresql':
        dbm = DBManagerPostgresql(
            db_type=db_type, database=database, duplicate_handling=duplicate_handling,
            transaction_size=transaction_size, cumulus_filter_dbm=cumulus_filter_dbm
        )
    else:
        dbm = DBManagerSQLite(
            db_type=db_type, database=database, duplicate_handling=duplicate_handling,
            transaction_size=transaction_size, cumulus_filter_dbm=cumulus_filter_dbm
        )
    return dbm


if __name__ == '__main__':
    pass
