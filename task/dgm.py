from abc import ABC, abstractmethod

from task.dgm_cumulus import DBManagerCumulus
from task.dgm_postgresql import DBManagerPostgresql
from task.dgm_sqlite import DBManagerSQLite


def get_db_manager(db_type, database=None, duplicate_handling='skip', transaction_size=100000):
    print(f'Creating {db_type} database manager')

    if db_type == 'cumulus':
        dbm = DBManagerCumulus(
            db_type=db_type, duplicate_handling=duplicate_handling, transaction_size=transaction_size, database=database
        )
    elif db_type == 'postgresql':
        dbm = DBManagerPostgresql(
            db_type=db_type, duplicate_handling=duplicate_handling, transaction_size=transaction_size, database=database
        )
    else:
        dbm = DBManagerSQLite(
            db_type=db_type, database=database, duplicate_handling=duplicate_handling, transaction_size=transaction_size
        )
    return dbm


if __name__ == '__main__':
    pass
