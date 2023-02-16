import datetime

from playhouse.apsw_ext import APSWDatabase, DateTimeField, CharField, Model, chunked, IntegerField, EXCLUDED

SQLITE_VAR_LIMIT = 999
db = APSWDatabase(None, vfs='unix-excl')


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
    db.close()


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

        db.close()
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
        db.close()

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

        db.close()
        return count

    def __insert_many(self, granule_dict, conflict_resolution, **kwargs):
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
                num = self.insert_many(key_batch, fields=fields).on_conflict(**conflict_resolution).execute()
                records_inserted += num

        db.close()
        return records_inserted


if __name__ == '__main__':
    pass
