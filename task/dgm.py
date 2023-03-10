import datetime
import itertools
import os
import time

from playhouse.apsw_ext import APSWDatabase, DateTimeField, CharField, Model, chunked, IntegerField, EXCLUDED

SQLITE_VAR_LIMIT = 999
db = APSWDatabase(None, vfs='unix-excl')


def initialize_db(db_file_path):
    db.init(
        db_file_path,
        timeout=900,
        pragmas={
            'journal_mode': 'wal',
            'cache_size': os.getenv('sqlite_cache_size'),
            'temp_store': os.getenv('sqlite_temp_store')
        }
    )
    db.create_tables([Granule], safe=True)


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

    def delete_granules_by_names(self, granule_names, **kwargs):
        """
        Removes all granule records from the database if the name is found in granule_names.
        :return del_count: The number of deleted granules
        """
        del_count = 0
        for key_batch in chunked(granule_names, SQLITE_VAR_LIMIT):
            delete = Granule.delete().where(Granule.name.in_(key_batch)).execute()
            del_count += delete

        return del_count

    def fetch_batch(self, collection_id, provider_path, batch_size=1000, **kwargs):
        """
        Fetches N files for up to batch_size granules for the provided collection_id and if the provider path
        is present in the full path of the file.
        :param collection_id: The id of the collection to fetch files for
        :param provider_path: The location where the granule files were discovered from
        :param batch_size: The limit for the number of unique granules to fetch files for
        :return: Returns a list of records that had the status set from "discovered" to queued
        """
        sub_query = (
            self.select(Granule.granule_id).distinct().where(
                (Granule.status == 'discovered') &
                (Granule.collection_id == collection_id) &
                (Granule.name.contains(provider_path))
            ).order_by(Granule.discovered_date.asc()).limit(batch_size)
        )

        update = (self.update(status='queued').where(Granule.granule_id.in_(sub_query)).returning(Granule))
        updated_records = list(update.execute())

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
        ).count()

        return count

    def data_generator(self, granule_dict):
        """
        Generator for query tuples
        :param granule_dict: Discover granules dictionary to insert into the database
        :yield: Insertable tuple
        """
        for k, v in granule_dict.items():
            yield (k, v['ETag'], v['GranuleId'], v['CollectionId'], 'discovered', v['Last-Modified'], v['Size'])

    def query_chunker(self, granule_dict, var_limit):
        """
        Breaks up the queries into subsets that will not overwhelm the Sqlite var limit
        :yield: Generator of a list
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
        records_inserted = 0
        fields = [Granule.name, Granule.etag, Granule.granule_id, Granule.collection_id, Granule.status,
                  Granule.last_modified, Granule.size]

        var_limit = SQLITE_VAR_LIMIT // len(fields)
        db_st = time.time()
        with db.atomic():
            for batch in self.query_chunker(granule_dict, var_limit):
                num = self.insert_many(batch, fields=fields).on_conflict(**conflict_resolution).execute()
                records_inserted += num
        db_et = time.time() - db_st
        print(f'Processed {records_inserted} records in {db_et} seconds.')
        print(f'Rate: {int(len(granule_dict) / db_et)}/s')
        return records_inserted


if __name__ == '__main__':
    pass
