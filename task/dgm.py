import datetime
import logging
from typing import Callable

from playhouse.apsw_ext import APSWDatabase, DateTimeField, CharField, Model, BooleanField, chunked, Check, IntegerField
from playhouse.migrate import SqliteMigrator, migrate

SQLITE_VAR_LIMIT = 999
db = APSWDatabase(None, vfs='unix-excl')


def safe_call(db_file_path, function: Callable, **kwargs):
    kwargs.get('logger').error(f'kwargs: {kwargs}')
    with initialize_db(db_file_path):
        ret = function(Granule(), **kwargs)
    return ret


def initialize_db(db_file_path):
    db.init(db_file_path, timeout=60, pragmas={
        'journal_mode': 'wal',
        'cache_size': -1 * 64000})
    db.create_tables([Granule], safe=True)

    # granule_id = CharField()
    # collection_id = CharField()
    # discovered_date = DateTimeField(default=datetime.datetime.now)
    # if granule_id not in db.get_columns(Granule):
    #     # migrate_tables(db)
    #     logging.info(f'Databse has not been migrated. Adding columns: {granule_id}, {collection_id}, {discovered_date}')
    #     migrator = SqliteMigrator(db)
    #     migrate(
    #         migrator.add_column('Granule', 'granule_id', granule_id),
    #         migrator.add_column('Granule', 'collection_id', collection_id),
    #         migrator.add_column('Granule', 'discover_date', discovered_date)
    #     )
    return db


class Granule(Model):
    """
    Model representing a granule and the associated metadata
    """
    name = CharField(primary_key=True)
    granule_id = CharField()
    collection_id = CharField()
    status = CharField()
    etag = CharField()
    last_modified = CharField()
    discovered_date = DateTimeField(default=datetime.datetime.now)
    size = IntegerField()

    class Meta:
        database = db

    def db_skip(self, granule_dict, **kwargs):
        """
        Inserts all the granules in the granule_dict unless they already exist
        :param granule_dict: Dictionary containing granules.
        """
        return self.__insert_many(granule_dict, 'ignore')

    def db_replace(self, granule_dict, **kwargs):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        :param granule_dict: Dictionary containing granules.
        """
        return self.__insert_many(granule_dict, 'replace')

    def db_error(self, granule_dict, **kwargs):
        """
        Tries to insert all the granules in the granule_dict erroring if there are duplicates
        :param granule_dict: Dictionary containing granules
        """
        return self.__insert_many(granule_dict, 'rollback')

    @staticmethod
    def delete_granules_by_names(granule_names, **kwargs):
        """
        Removes all granule records from the database if the name is found in granule_names.
        :return del_count: The number of deleted granules
        """
        del_count = 0
        for key_batch in chunked(granule_names, SQLITE_VAR_LIMIT):
            delete = Granule.delete().where(Granule.name.in_(key_batch)).execute()
            del_count += delete
        return del_count

    def fetch_batch(self, collection_id, batch_size=1000, **kwargs):
        select_granule_ids = (Granule.select(Granule.granule_id).order_by(Granule.discovered_date).limit(batch_size).where(
            (Granule.status == 'discovered') &
            (Granule.collection_id == collection_id)))

        for granule_id in select_granule_ids:
            kwargs.get('logger').info(f'select_granule_ids: {granule_id.granule_id}')
        batch_results = Granule.select().where(Granule.granule_id.in_(select_granule_ids)).execute()
        _ = Granule.update(status='queued').where(Granule.granule_id.in_(select_granule_ids)).execute()

        for result in batch_results:
            print(f'batch entry: {result}')
        return batch_results

    @staticmethod
    def __insert_many(granule_dict, conflict_resolution, **kwargs):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param granule_dict: Dictionary containing granules
        """
        records_inserted = 0
        data = [(k, v['ETag'], v['GranuleId'], v['CollectionId'], 'discovered', v['Last-Modified'], v['Size']) for k, v in granule_dict.items()]
        fields = [Granule.name, Granule.etag, Granule.granule_id, Granule.collection_id, Granule.status, Granule.last_modified, Granule.size]
        with db.atomic():
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                num = Granule.insert_many(key_batch, fields=fields).on_conflict(conflict_resolution).execute()
                records_inserted += num

        return records_inserted


class Temp:
    def a_method(self, param1, **kwargs):
        pass
def caller(callable, **kwargs):
    callable(Temp(), **kwargs)

if __name__ == '__main__':
    at = Temp()

    caller()
    pass
