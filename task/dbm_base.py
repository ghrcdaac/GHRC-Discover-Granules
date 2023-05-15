import itertools
import time
from abc import ABC, abstractmethod

TABLE_NAME = 'granule'


class DBManagerBase(ABC):
    def __init__(self, duplicate_handling='skip', transaction_size=100000, **kwargs):
        self.dict_list = []
        self.discovered_files_count = 0
        self.queued_files_count = 0
        self.duplicate_handling = duplicate_handling
        self.transaction_size = transaction_size

    @abstractmethod
    def close_db(self):
        raise NotImplementedError

    def add_record(self, name, granule_id, collection_id, etag, last_modified, size):
        self.dict_list.append({
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
    def read_batch(self, collection_id, provider_path, batch_size):
        raise NotImplementedError


class DBManagerPeewee(DBManagerBase):
    def __init__(self, model_class, database, auto_batching, transaction_size, duplicate_handling, cumulus_filter,
                 var_limit, excluded, chunked, **kwargs):
        super().__init__(duplicate_handling, transaction_size, **kwargs)
        self.model_class = model_class
        self.database = database
        self.auto_batching = auto_batching
        self.list_dict = []
        self.cumulus_filter = cumulus_filter
        self.var_limit = var_limit
        self.discovered_files_count = 0
        self.queued_files_count = 0

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
        self.write_batch()

    def write_batch(self):
        if self.cumulus_filter and self.duplicate_handling == 'skip':
            print('Using cumulus filter')
            discovered_granule_ids = tuple(x.get('granule_id') for x in self.list_dict)
            new_granule_ids = self.cumulus_filter.filter_against_cumulus(discovered_granule_ids)

            index = 0
            while index < len(self.list_dict):
                record = self.list_dict[index]
                if record.get('granule_id') not in new_granule_ids:
                    self.list_dict.pop(index)
                else:
                    index += 1

        print(f'Writing batch to database...')
        print(self.list_dict)
        if len(self.list_dict) > 0:
            if self.duplicate_handling == 'skip':
                self.discovered_files_count += self.db_skip()
            elif self.duplicate_handling == 'replace':
                print(type(self))
                self.discovered_files_count += self.db_replace()
            else:
                print('bad wrong')
                self.discovered_files_count += getattr(self, f'db_{self.duplicate_handling}')(self.list_dict)
            self.list_dict.clear()

    def close_db(self):
        self.database.close()

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

    def read_batch(self, collection_id, provider_full_url, batch_size=1000):
        """
        Fetches N files for up to batch_size granules for the provided collection_id and if the provider path
        is present in the full path of the file.
        :param collection_id: The id of the collection to fetch files for
        :param provider_full_url: The location where the granule files were discovered from
        :param batch_size: The limit for the number of unique granules to fetch files for
        :return: Returns a list of records that had the status set from "discovered" to queued
        """
        sub_query = (
            self.model_class.select(self.model_class.granule_id).dicts().where(
                (self.model_class.status == 'discovered') &
                (self.model_class.collection_id == collection_id) &
                (self.model_class.name.contains(provider_full_url))
            ).order_by(self.model_class.discovered_date.asc()).limit(batch_size)
        )

        update = (self.model_class.update(status='queued').where(
            (self.model_class.granule_id.in_(sub_query)) &
            (self.model_class.name.contains(provider_full_url))
        ).returning(self.model_class).dicts())
        print(f'Update query: {update}')
        updated_records = list(update.execute())
        print(f'Records returned by query: {len(updated_records)}')

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

    @staticmethod
    def data_generator(granule_dict):
        """
        Generator for query tuples
        :param granule_dict: Discover granules dictionary to insert into the database
        :yield: Insertable tuple
        """
        for k, v in granule_dict.items():
            yield k, v['etag'], v['granule_id'], v['collection_id'], 'discovered', v['last_modified'], v['size']

    @staticmethod
    def query_chunker(granule_dict, var_limit):
        """
        Breaks up the queries into subsets that will not overwhelm the query var limit
        :yield: Generator of a list of tuples
        """
        batch_continue = True
        data_generator = DBManagerPeewee.data_generator(granule_dict)
        while batch_continue:
            batch_list = list(itertools.islice(data_generator, var_limit))
            if len(batch_list) < var_limit:
                batch_continue = False

            if len(batch_list) == var_limit or (batch_continue is False and len(batch_list) > 0):
                yield batch_list
                batch_list.clear()

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
                print(batch)
                num = self.model_class.insert_many(batch).on_conflict(**conflict_resolution).execute()
                if isinstance(num, int):
                    records_inserted += num
                else:
                    records_inserted += len(num)
        db_et = time.time() - db_st
        print(f'Inserted {records_inserted}/{len(self.list_dict)} records in {db_et} seconds.')
        print(f'Rate: {int(len(self.list_dict) / db_et)}/s')
        return records_inserted
