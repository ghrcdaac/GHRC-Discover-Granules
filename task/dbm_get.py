from task.dbm_cumulus import get_db_manager_cumulus
from task.dbm_postgresql import get_db_manager_psql
from task.dbm_sqlite import get_db_manager_sqlite


def get_db_manager(
        collection_id, provider_url, db_type, batch_limit, database=None, duplicate_handling='skip',
        transaction_size=100000, cumulus_filter_dbm=None, file_count=1
):
    print(f'Creating {db_type} database manager with cumulus_filtering {True if cumulus_filter_dbm else False}')

    if db_type == 'cumulus':
        dbm = get_db_manager_cumulus(
            collection_id=collection_id, database=database, duplicate_handling=duplicate_handling,
            batch_limit=batch_limit, transaction_size=transaction_size
        )
    elif db_type == 'postgresql':
        dbm = get_db_manager_psql(
            collection_id=collection_id, provider_full_url=provider_url, database=database,
            duplicate_handling=duplicate_handling, batch_limit=batch_limit,
            transaction_size=transaction_size, cumulus_filter=cumulus_filter_dbm, file_count=file_count
        )
    else:
        dbm = get_db_manager_sqlite(
            collection_id=collection_id, provider_full_url=provider_url, database=database,
            duplicate_handling=duplicate_handling, batch_limit=batch_limit,
            transaction_size=transaction_size, cumulus_filter=cumulus_filter_dbm, file_count=file_count
        )

    return dbm


if __name__ == '__main__':
    pass
