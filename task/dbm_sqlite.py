import datetime
import os

from playhouse.apsw_ext import APSWDatabase, CharField, DateTimeField, Model, EXCLUDED, chunked, BigIntegerField

from task.dbm_base import DBManagerPeewee, TABLE_NAME

DB_SQLITE = APSWDatabase(None, vfs='unix-excl')
VAR_LIMIT_SQLITE = 999


def get_db_manager_sqlite(database, **kwargs):
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

    return DBManagerSqlite(DB_SQLITE, GranuleSQLite, **kwargs)


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
    def __init__(self, database, model_class, **kwargs):
        self.model_class = model_class
        super().__init__(database, model_class, VAR_LIMIT_SQLITE, EXCLUDED, chunked, **kwargs)

    def db_replace(self):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        """
        conflict_handling = {'action': 'replace'}
        return self.insert_many(conflict_handling)


if __name__ == '__main__':
    pass
