import datetime
import itertools
import os
import time

from task.dbm_base import DBManagerBase, DBManagerWriter
from playhouse.apsw_ext import APSWDatabase, DateTimeField, CharField, Model, IntegerField, EXCLUDED, chunked

from task.dgm_cumulus import DBManagerCumulus

DB = APSWDatabase(None, vfs='unix-excl')
VAR_LIMIT = 999


class DBManagerSQLite(DBManagerWriter):
    def __init__(
            self, db_type, database, duplicate_handling, transaction_size, force_replace=False, cumulus_filter=False,
            **kwargs
    ):
        super().__init__(db_type, duplicate_handling, transaction_size, **kwargs)
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
        DB.init(**db_init_kwargs)
        DB.create_tables([Granule], safe=True)
        self.force_replace = force_replace
        self.model = Granule()
        self.cumulus_filter = cumulus_filter
        if self.cumulus_filter:
            self.cumulus_dbm = DBManagerCumulus(
                db_type='cumulus', database=None, transaction_size=transaction_size, duplicate_handling='replace'
            )

    def close_db(self):
        if self.db_type != ':memory:':
            DB.close()


# TODO: The granule model needs to be refactored to prevent the current code duplication. This is complicated by
#       the models necessity to have the database defined at a global scope and an incompatibility between field types
#       for SQLite and Postgresql databases. Additionally the database must be defined in a global scope for the
#       transaction logic in the write_many function.
class Granule(Model):
    """
    Model representing a granule and the associated metadata
    """
    name = CharField(primary_key=True)
    granule_id = CharField()
    collection_id = CharField()
    status = CharField(default='discovered')
    etag = CharField()
    last_modified = CharField()
    discovered_date = DateTimeField(formats='YYYY-mm-dd HH:MM:SS', default=datetime.datetime.now)
    size = IntegerField()

    class Meta:
        database = DB

    def db_skip(self, granule_dict, **kwargs):
        """
        Inserts all the granules in the granule_dict unless they already exist
        :param granule_dict: Dictionary containing granules.
        """
        conflict_resolution = {
            'conflict_target': [Granule.name],
            'update': {
                Granule.etag: EXCLUDED.etag,
                Granule.last_modified: EXCLUDED.last_modified,
                Granule.discovered_date: EXCLUDED.discovered_date,
                Granule.status: EXCLUDED.status,
                Granule.size: EXCLUDED.size
            },
            'where': (
                    (Granule.etag != EXCLUDED.etag) |
                    (Granule.last_modified != EXCLUDED.last_modified) |
                    (Granule.size != EXCLUDED.size)
            )
        }
        return self.__insert_many(granule_dict, conflict_resolution)

    def db_replace(self, granule_dict, **kwargs):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        :param granule_dict: Dictionary containing granules.
        """
        conflict_handling = {'action': 'replace'}
        return self.__insert_many(granule_dict, conflict_handling)

    def db_error(self, granule_dict, **kwargs):
        """
        Tries to insert all the granules in the granule_dict erroring if there are duplicates
        :param granule_dict: Dictionary containing granules
        """
        return self.__insert_many(granule_dict, {'action': 'rollback'})

    def read_batch(self, collection_id, provider_path, batch_size=1000):
        """
        Fetches N files for up to batch_size granules for the provided collection_id and if the provider path
        is present in the full path of the file.
        :param collection_id: The id of the collection to fetch files for
        :param provider_path: The location where the granule files were discovered from
        :param batch_size: The limit for the number of unique granules to fetch files for
        :return: Returns a list of records that had the status set from "discovered" to queued
        """
        sub_query = (
            self.select(Granule.granule_id).dicts().where(
                (Granule.status == 'discovered') &
                (Granule.collection_id == collection_id) &
                (Granule.name.contains(provider_path))
            ).order_by(Granule.discovered_date.asc()).limit(batch_size)
        )

        update = (self.update(status='queued').where(
            (Granule.granule_id.in_(sub_query)) &
            (Granule.name.contains(provider_path))
        ).returning(Granule).dicts())
        print(f'Update query: {update}')
        updated_records = list(update.execute())
        print(f'Records returned by query: {len(updated_records)}')

        return updated_records

    def count_records(self, collection_id, provider_path, status='discovered', count_type='files'):
        """
        Counts the number of records that match the parameters passed in
        :param collection_id: The id of the collection to fetch files for
        :param provider_path: The location where the granule files were discovered from
        :param status: "discovered" if the records have now been part of a batch or "queued" if they have
        :param count_type: "files" to count the number of files or "granules" to count count granules. It should always
        be the case that granules <= files.
        :return: The number of records that matched
        """
        query = self.select(Granule.granule_id)

        if count_type == 'granules':
            query = query.distinct()

        count = query.where(
            (Granule.status == status) &
            (Granule.collection_id == collection_id) &
            (Granule.name.contains(provider_path))
        )

        return count.count()

    @staticmethod
    def data_generator(granule_dict):
        """
        Generator for query tuples
        :param granule_dict: Discover granules dictionary to insert into the database
        :yield: Insertable tuple
        """
        for k, v in granule_dict.items():
            yield k, v['etag'], v['granule_id'], v['collection_id'], 'discovered', v['last_modified'], v['size']

    def query_chunker(self, granule_dict, var_limit):
        """
        Breaks up the queries into subsets that will not overwhelm the query var limit
        :yield: Generator of a list of tuples
        """
        batch_continue = True
        data_generator = self.data_generator(granule_dict)
        while batch_continue:
            batch_list = list(itertools.islice(data_generator, var_limit))
            if len(batch_list) < var_limit:
                batch_continue = False

            if len(batch_list) == var_limit or (batch_continue is False and len(batch_list) > 0):
                yield batch_list
                batch_list.clear()

    def __insert_many(self, granule_dict, conflict_resolution, **kwargs):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param granule_dict: Dictionary containing granules
        """
        print(f'Inserting {len(granule_dict)} records...')
        records_inserted = 0
        fields = [Granule.name, Granule.etag, Granule.granule_id, Granule.collection_id, Granule.last_modified,
                  Granule.size]

        var_limit = VAR_LIMIT // len(fields)
        db_st = time.time()
        with DB.atomic():
            for batch in chunked(granule_dict, var_limit):
                # print(batch)
                num = self.insert_many(batch).on_conflict(**conflict_resolution).execute()
                if isinstance(num, int):
                    records_inserted += num
                else:
                    records_inserted += len(num)
        db_et = time.time() - db_st
        print(f'Inserted {records_inserted}/{len(granule_dict)} records in {db_et} seconds.')
        print(f'Rate: {int(len(granule_dict) / db_et)}/s')
        return records_inserted


if __name__ == '__main__':
    pass
