import datetime
from typing import Callable

from playhouse.apsw_ext import APSWDatabase, DateTimeField, CharField, Model, chunked, IntegerField, EXCLUDED

SQLITE_VAR_LIMIT = 999
db = APSWDatabase(None, vfs='unix-excl')


def safe_call(db_file_path, function: Callable, **kwargs):
    with initialize_db(db_file_path):
        ret = function(Granule(), **kwargs)
    return ret


def initialize_db(db_file_path):
    db.init(
        db_file_path,
        timeout=900,
        pragmas={
            'journal_mode': 'wal',
            'cache_size': -1 * 64000
        }
    )
    db.create_tables([Granule], safe=True)

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
        conflict_resolution = {
            'conflict_target': [Granule.name],
            'preserve': [Granule.etag, Granule.last_modified, Granule.discovered_date, Granule.status, Granule.size],
            'where': (EXCLUDED.etag != Granule.etag)
        }
        return self.__insert_many(granule_dict, conflict_resolution)

    def db_replace(self, granule_dict, **kwargs):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        :param granule_dict: Dictionary containing granules.
        """
        return self.__insert_many(granule_dict, {'action': 'replace'})

    def db_error(self, granule_dict, **kwargs):
        """
        Tries to insert all the granules in the granule_dict erroring if there are duplicates
        :param granule_dict: Dictionary containing granules
        """
        return self.__insert_many(granule_dict, {'action': 'rollback'})

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

    # This function cannot be made static
    def fetch_batch(self, collection_id, provider_path, batch_size=1000, **kwargs):
        sub_query = (
            Granule.select().order_by(Granule.discovered_date).limit(batch_size).where(
                (Granule.status == 'discovered') &
                (Granule.collection_id == collection_id) &
                (Granule.name.contains(provider_path)))
        )

        count = list(Granule.update(status='queued').where(Granule.name.in_(sub_query)).returning(Granule).execute())
        return count

    # This function cannot be made static
    def count_discovered(self, collection_id, provider_path):
        return Granule.select(Granule.granule_id).where(
            (Granule.status == 'discovered') &
            (Granule.collection_id == collection_id) &
            (Granule.name.contains(provider_path))
        ).count()

    @staticmethod
    def __insert_many(granule_dict, conflict_resolution, **kwargs):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param granule_dict: Dictionary containing granules
        """
        records_inserted = 0
        data = [(k, v['ETag'], v['GranuleId'], v['CollectionId'], 'discovered', v['Last-Modified'], v['Size']) for k, v
                in granule_dict.items()]
        fields = [Granule.name, Granule.etag, Granule.granule_id, Granule.collection_id, Granule.status,
                  Granule.last_modified, Granule.size]
        with db.atomic():
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                num = Granule.insert_many(key_batch, fields=fields).on_conflict(**conflict_resolution).execute()
                records_inserted += num

        return records_inserted


if __name__ == '__main__':
    pass
