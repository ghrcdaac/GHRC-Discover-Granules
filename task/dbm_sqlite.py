import datetime
import os

from playhouse.apsw_ext import APSWDatabase, CharField, DateTimeField, Model, EXCLUDED, chunked, BigIntegerField

from task.dbm_base import DBManagerPeewee, TABLE_NAME

DB_SQLITE = APSWDatabase(None, vfs='unix-excl')
VAR_LIMIT_SQLITE = 999


def get_db_manager_sqlite(collection_id, provider_full_url, database, duplicate_handling, batch_limit, transaction_size,
                          cumulus_filter=None, auto_batching=True):
    global DB_SQLITE
    db_init_kwargs = {
        'database': database,
        'timeout': 900,
        'vfs': 'unix-excl',
        'pragmas': {
            'journal_mode': 'wal',
            'cache_size': os.getenv('sqlite_cache_size'),
            'temp_store': os.getenv('sqlite_temp_store')
        }
    }
    DB_SQLITE.init(**db_init_kwargs)
    DB_SQLITE.create_tables([GranuleSQLite], safe=True)

    return DBManagerSqlite(
        collection_id, provider_full_url, GranuleSQLite, DB_SQLITE, auto_batching, batch_limit, transaction_size,
        duplicate_handling, cumulus_filter
    )


class GranuleSQLite(Model):
    name = CharField(primary_key=True)
    granule_id = CharField()
    collection_id = CharField()
    status = CharField(default='discovered')
    etag = CharField()
    last_modified = CharField()
    discovered_date = DateTimeField(formats='YYYY-mm-dd HH:MM:SS', default=datetime.datetime.now)
    size = BigIntegerField()

    class Meta:
        database = DB_SQLITE
        table_name = TABLE_NAME


class DBManagerSqlite(DBManagerPeewee):
    def __init__(
            self, collection_id, provider_full_url, model_class, database, auto_batching, batch_limit, transaction_size,
            duplicate_handling, cumulus_filter
    ):
        self.model_class = model_class
        super().__init__(
            collection_id, provider_full_url, self.model_class, database, auto_batching, batch_limit, transaction_size,
            duplicate_handling, cumulus_filter, VAR_LIMIT_SQLITE, EXCLUDED, chunked
        )

    def db_replace(self):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        """
        conflict_handling = {'action': 'replace'}
        return self.insert_many(conflict_handling)


if __name__ == '__main__':
    pass
