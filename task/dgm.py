import datetime
import logging
from typing import Callable

from playhouse.apsw_ext import APSWDatabase, DateTimeField, CharField, Model, BooleanField, chunked
from playhouse.migrate import SqliteMigrator, migrate

SQLITE_VAR_LIMIT = 999
db = APSWDatabase(None, vfs='unix-excl')


def safe_call(db_file_path, function: Callable, **kwargs):
    with initialize_db(db_file_path):
        ret = function(Granule(), **kwargs)
    return ret


def initialize_db(db_file_path):
    db.init(db_file_path, timeout=60, pragmas={
        'journal_mode': 'wal',
        'cache_size': -1 * 64000})
    db.create_tables([Granule], safe=True)

    granule_id = CharField()
    # if granule_id not in db.get_columns(Granule):
    #     logging.info(f'Databse has not been migrated. Adding column {granule_id}')
    #     migrator = SqliteMigrator(db)
    #     granule_id = CharField()
    #     migrate(
    #         migrator.add_column('Granule', 'granule_id', granule_id)
    #     )
    return db


class Granule(Model):
    """
    Model representing a granule and the associated metadata
    """
    name = CharField(primary_key=True)
    granule_id = CharField()
    collection_id = CharField()
    queued = BooleanField()
    etag = CharField()
    last_modified = CharField()
    discovered_date = DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = db

    @staticmethod
    def select_all(granule_dict, **kwargs):
        """
        Selects all records from the database that are an exact match for all three fields
        :param granule_dict: Dictionary containing granules.
        :return ret_lst: List of granule names that existed in the database
        """
        ret_lst = []

        fields = [Granule.name, Granule.granule_id, Granule.collection_id, Granule.queued, Granule.etag, Granule.last_modified]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            granule_ids = set()
            etags = set()
            last_mods = set()

            for key in key_batch:
                names.add(key)
                granule_ids.add((granule_dict[key]["GranuleId"]))
                etags.add(granule_dict[key]["ETag"])
                last_mods.add(granule_dict[key]["Last-Modified"])

            sub = Granule\
                .select(Granule.name)\
                .where(Granule.name.in_(names) & Granule.etag.in_(etags) & Granule.last_modified.in_(last_mods))
            for name in sub.tuples().iterator():
                ret_lst.append(name[0])

        return ret_lst

    def db_skip(self, granule_dict, **kwargs):
        """
        Inserts all the granules in the granule_dict unless they already exist
        :param granule_dict: Dictionary containing granules.
        """
        for name in self.select_all(granule_dict):
            granule_dict.pop(name)
        return self.__insert_many(granule_dict)

    def db_replace(self, granule_dict, **kwargs):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        :param granule_dict: Dictionary containing granules.
        """
        return self.__insert_many(granule_dict)

    def db_error(self, granule_dict, **kwargs):
        """
        Tries to insert all the granules in the granule_dict erroring if there are duplicates
        :param granule_dict: Dictionary containing granules
        """
        fields = [Granule.name]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            for key in key_batch:
                names.add(key)
            res = Granule.select(Granule.name).where(Granule.name.in_(names))
            if res:
                raise ValueError('Granule already exists in the database.')

        return self.__insert_many(granule_dict)

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

    @staticmethod
    def fetch_batch(batch_size=1000, **kwargs):
        select_granule_ids = Granule.select().distinct(Granule.granule_id)\
            .order_by(Granule.discovered_date)\
            .limit(batch_size)\
            .where(Granule.queued is False)

        batch_results = Granule.select().where(Granule.granule_id.in_(select_granule_ids)).execute()

        for result in batch_results:
            print(f'batch entry: {result}')
        return batch_results

    @staticmethod
    def __insert_many(granule_dict, **kwargs):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param granule_dict: Dictionary containing granules
        """
        records_inserted = 0
        data = [(k, v['ETag'], v['GranuleId'], v['CollectionId'], False, v['Last-Modified']) for k, v in granule_dict.items()]
        fields = [Granule.name, Granule.etag, Granule.granule_id, Granule.collection_id, Granule.queued, Granule.last_modified]
        with db.atomic():
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                num = Granule.insert_many(key_batch, fields=fields).on_conflict_replace().execute()
                records_inserted += num

        return records_inserted


if __name__ == '__main__':
    pass
