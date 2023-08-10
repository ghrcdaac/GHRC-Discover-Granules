import time
from abc import ABC, abstractmethod

TABLE_NAME = 'granule'


class DBManagerBase(ABC):
    def __init__(self, duplicate_handling='skip', batch_limit=1000, transaction_size=100000, **kwargs):
        self.list_dict = []
        self.discovered_files_count = 0
        self.queued_files_count = 0
        self.duplicate_handling = duplicate_handling
        self.batch_limit = batch_limit
        self.transaction_size = transaction_size

    @abstractmethod
    def close_db(self):
        raise NotImplementedError

    def add_record(self, name, granule_id, collection_id, etag, last_modified, size):
        self.list_dict.append({
            'name': name,
            'etag': etag,
            'granule_id': granule_id,
            'collection_id': collection_id,
            'last_modified': str(last_modified),
            'size': size
        })

    @abstractmethod
    def flush_dict(self):
        raise NotImplementedError

    @abstractmethod
    def read_batch(self):
        raise NotImplementedError


class DBManagerPeewee(DBManagerBase):
    def __init__(self, collection_id, provider_full_url, model_class, database, auto_batching, batch_limit,
                 transaction_size, duplicate_handling, cumulus_filter, var_limit, excluded, chunked, **kwargs):
        super().__init__(duplicate_handling, batch_limit, transaction_size, **kwargs)
        self.model_class = model_class
        self.database = database
        self.auto_batching = auto_batching
        self.list_dict = []
        self.cumulus_filter = cumulus_filter
        self.var_limit = var_limit
        self.discovered_files_count = 0
        self.queued_files_count = 0
        self.collection_id = collection_id
        self.provider_full_url = provider_full_url

        self.excluded = excluded
        self.chunked = chunked

    def add_record(self, name, granule_id, collection_id, etag, last_modified, size):
        self.list_dict.append({
            'name': name,
            'etag': etag,
            'granule_id': granule_id,
            'collection_id': collection_id,
            'last_modified': str(last_modified),
            'size': size
        })

        if self.auto_batching and len(self.list_dict) >= self.transaction_size:
            self.write_batch()

        return self.transaction_size - len(self.list_dict)

    def flush_dict(self):  # TODO: Rename to list
        return self.write_batch()

    def write_batch(self):
        records_inserted = 0
        if self.cumulus_filter and self.duplicate_handling == 'skip' and self.list_dict:
            print('Filtering discovered granules against cumulus granule IDs...')
            discovered_granule_ids = tuple(x.get('granule_id') for x in self.list_dict)
            cumulus_granule_id_set = self.cumulus_filter.filter_against_cumulus(discovered_granule_ids)

            index = 0
            while index < len(self.list_dict):
                record = self.list_dict[index]
                if record.get('granule_id') in cumulus_granule_id_set:
                    self.list_dict.pop(index)
                else:
                    index += 1
            print(f'Records remain after filtering: {len(self.list_dict)}')

        if len(self.list_dict) > 0:
            if self.cumulus_filter or self.duplicate_handling == 'replace':
                print('Writing batch to database using replace...')
                records_inserted = self.db_replace()
            elif self.duplicate_handling == 'skip':
                print('Writing batch to database using skip...')
                records_inserted = self.db_skip()
            else:
                raise ValueError(f'Batch not inserted into the database. This should not have happened.'
                                 f'duplicate_handling: {self.duplicate_handling} '
                                 f'cumulus_filter: {self.cumulus_filter}')

            self.list_dict.clear()

        self.discovered_files_count += records_inserted
        return records_inserted

    def close_db(self):
        self.database.close()
        if self.cumulus_filter:
            self.cumulus_filter.close_db()

    def db_replace(self):
        raise NotImplementedError

    def db_skip(self):
        """
        Inserts all the granules in the granule_dict unless they already exist
        """
        conflict_resolution = {
            'conflict_target': [self.model_class.name],
            'update': {
                self.model_class.etag: self.excluded.etag,
                self.model_class.last_modified: self.excluded.last_modified,
                self.model_class.discovered_date: self.excluded.discovered_date,
                self.model_class.status: self.excluded.status,
                self.model_class.size: self.excluded.size
            },
            'where': (
                    (self.model_class.etag != self.excluded.etag) |
                    (self.model_class.last_modified != self.excluded.last_modified) |
                    (self.model_class.size != self.excluded.size)
            )
        }
        return self.insert_many(conflict_resolution)

    def db_error(self):
        """
        Tries to insert all the granules in the granule_dict erroring if there are duplicates
        """
        return self.insert_many({'action': 'rollback'})

    @staticmethod
    def add_for_update(select_query):
        """
        SELECT * FOR UPDATE is not supported for SQLite so just return the query.
        :param select_query: The subquery for an update query.
        :return: The unmodified subquery
        """
        return select_query

    def read_batch(self):
        """
        Fetches up to batch_limit file records for the provided collection_id and if the provider path
        is present in the full path of the file database name field.
        :return: Returns a list of records that had the status set from "discovered" to queued
        """
        # Note: The presence of order_by in the subquery is to ensure the oldest granule IDs are fetched first but the
        # order is not preserved in the wrapping query.
        sub_query = (
            self.model_class.select(self.model_class.granule_id).where(
                (self.model_class.status == 'discovered') &
                (self.model_class.collection_id == self.collection_id) &
                (self.model_class.name.startswith(self.provider_full_url))
            ).order_by(self.model_class.discovered_date.asc()).limit(self.batch_limit)
        )

        sub_query = self.add_for_update(sub_query)

        update = (self.model_class.update(status='queued').where(
            (self.model_class.granule_id.in_(sub_query)) &
            (self.model_class.name.startswith(self.provider_full_url)) &
            (self.model_class.collection_id == self.collection_id)
        ).returning(self.model_class).dicts())

        print(f'Update query: {update}')
        st = time.time()
        updated_records = list(update.execute())
        et = time.time() - st
        print(f'Updated {len(updated_records)} records in {et} seconds.')
        print(f'Rate: {int(len(updated_records) / et)}/s')

        self.queued_files_count += len(updated_records)
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
        query = self.model_class.select(self.model_class.granule_id)

        if count_type == 'granules':
            query = query.distinct()

        count = query.where(
            (self.model_class.status == status) &
            (self.model_class.collection_id == collection_id) &
            (self.model_class.name.contains(provider_path))
        )

        return count.count()

    def insert_many(self, conflict_resolution):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param conflict_resolution: conflict resolution object
        """
        print(f'Inserting {len(self.list_dict)} records...')
        records_inserted = 0

        field_count = 8
        var_limit = self.var_limit // field_count
        db_st = time.time()
        with self.database.atomic():
            for batch in self.chunked(self.list_dict, var_limit):
                # print(batch)
                num = self.model_class.insert_many(batch).on_conflict(**conflict_resolution).execute()
                if isinstance(num, int):
                    records_inserted += num
                else:
                    records_inserted += len(num)
        db_et = time.time() - db_st
        print(f'Inserted {records_inserted}/{len(self.list_dict)} records in {db_et} seconds.')
        print(f'Rate: {int(len(self.list_dict) / db_et)}/s')
        return records_inserted
