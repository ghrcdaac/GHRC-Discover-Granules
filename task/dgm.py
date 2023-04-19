import datetime
import itertools
import json
import os
import time
from abc import ABC, abstractmethod

import boto3

DB_TYPE = os.getenv('db_type')
VAR_LIMIT = 0

print(f'Database configuration: {DB_TYPE}')
if DB_TYPE == 'postgresql' or DB_TYPE == 'cumulus':
    import psycopg2
    VAR_LIMIT = 32768
    sm = boto3.client('secretsmanager')
    if DB_TYPE == 'postgresql':
        from playhouse.postgres_ext import PostgresqlExtDatabase, DateTimeField, CharField, Model, chunked, \
            IntegerField, EXCLUDED
        secrets_arn = os.getenv('postgresql_secret_arn', None)
        init_kwargs = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))

        DB = PostgresqlExtDatabase(None)
    elif DB_TYPE == 'cumulus':
        secrets_arn = os.getenv('cumulus_credentials_arn', None)
        init_kwargs = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))
        init_kwargs.update({'user': init_kwargs.pop('username')})
        DB = psycopg2.connect(**init_kwargs)
    else:
        print(f'db_type should be one of postgresql, cumulus, or sqlite but it was {DB_TYPE}')
else:
    from playhouse.apsw_ext import APSWDatabase, DateTimeField, CharField, Model, IntegerField, EXCLUDED
    DB = APSWDatabase(None, vfs='unix-excl')
    VAR_LIMIT = 999
    init_kwargs = {
        'timeout': 900,
        'vfs': 'unix-excl',
        'pragmas': {
            'journal_mode': 'wal',
            'cache_size': os.getenv('sqlite_cache_size'),
            'temp_store': os.getenv('sqlite_temp_store')
        }
    }


def get_db_manager_class(*args, **kwargs):
    print(f'Creating {DB_TYPE} database manager: {kwargs}')
    if DB_TYPE == 'cumulus':
        _ = kwargs.pop('database')
        return DBManagerCumulus(*args, **kwargs)
    elif DB_TYPE == 'postgresql' or DB_TYPE == 'sqlite':
        return DBManager(*args, **kwargs)
    else:
        return DBManagerNoDB()


class DBManagerBase(ABC):
    def __init__(self, duplicate_handling='skip', transaction_size=100000):
        self.dict_queue = {}
        self.discovered_granules_count = 0
        self.queued_files_count = 0
        self.duplicate_handling = duplicate_handling
        self.transaction_size = transaction_size

    @staticmethod
    def close_db():
        DB.close()

    @abstractmethod
    def add_record(self, dict_record):
        raise NotImplementedError

    @abstractmethod
    def flush_dict(self):
        raise NotImplementedError

    @abstractmethod
    def read_batch(self, collection_id, provider_path, batch_size):
        raise NotImplementedError


class DBManager(DBManagerBase):
    def __init__(self, database=None, duplicate_handling='skip', transaction_size=50000):
        super().__init__(duplicate_handling, transaction_size)

        if DB_TYPE == 'sqlite':
            DB.init(database=database, **init_kwargs)
            DB.create_tables([Granule], safe=True)
        else:
            DB.init(
                database=init_kwargs.get('database'),
                user=init_kwargs.get('user'),
                password=init_kwargs.get('password'),
                host=init_kwargs.get('host'),
                port=init_kwargs.get('port')
            )
        DB.create_tables([Granule], safe=True)
        self.model = Granule()

    def __del__(self):
        DB.close()

    def add_record(self, dict_record):
        self.dict_queue.update(dict_record)

        if len(self.dict_queue) >= self.transaction_size:
            self.write_batch()

        return self.transaction_size - len(self.dict_queue)

    def flush_dict(self):
        self.write_batch()

    def read_batch(self, collection_id, provider_path, batch_size):
        batch = self.model.read_batch(collection_id, provider_path, batch_size)
        self.queued_files_count += len(batch)
        return batch

    def write_batch(self):
        self.discovered_granules_count += getattr(self.model, f'db_{self.duplicate_handling}')(self.dict_queue)
        self.dict_queue.clear()


class DBManagerCumulus(DBManagerBase):
    def __init__(self, duplicate_handling='skip', transaction_size=50000):
        super().__init__(duplicate_handling, transaction_size)

    def add_record(self, dict_record):
        self.dict_queue.update(dict_record)
        return self.transaction_size - len(self.dict_queue)

    def flush_dict(self):
        if self.duplicate_handling == 'skip':
            db_granule_ids = self.trim_results()
            db_granule_ids = {x for x in db_granule_ids}
            print(f'Trimming {len(db_granule_ids)} files that already existed in the Cumulus database.')
            print(f'Trimmed granule IDs: {db_granule_ids}')

            # Get keys that need to be removed
            discard_set = set()
            for k, v in self.dict_queue.items():
                discovered_granule_id = v.get('GranuleId', None)
                if discovered_granule_id in db_granule_ids:
                    db_granule_ids.discard(discovered_granule_id)
                    discard_set.add(k)

            # Delete keys
            for key in discard_set:
                del self.dict_queue[key]

        self.discovered_granules_count += len(self.dict_queue)

    def read_batch(self, collection_id, provider_path, batch_size):
        self.queued_files_count += len(self.dict_queue)
        return self.dict_queue

    def trim_results(self, **kwargs):
        granule_ids = [x.get('GranuleId') for x in self.dict_queue.values()]
        results = []
        start_index = 0
        end_index = VAR_LIMIT
        db_st = time.time()
        while True:
            with DB.cursor() as curs:
                id_batch = tuple(granule_ids[start_index:end_index])
                if len(id_batch) == 0:
                    break
                query_string = 'SELECT granules.granule_id FROM granules WHERE granules.granule_id IN %s;'
                print(f'Query: {query_string}')
                curs.execute(query_string, (id_batch,))
                results.extend([x[0] for x in curs.fetchall()])
                start_index = end_index + 1
                end_index += VAR_LIMIT
        db_et = time.time() - db_st
        print(f'{len(results)} records read in {db_et} seconds')
        print(f'Rate: {int(len(results) / db_et)}/s')

        return results


class DBManagerNoDB(DBManagerBase):
    def add_record(self, dict_record):
        self.dict_queue.update({dict_record})

    def flush_dict(self):
        self.dict_queue.clear()

    def read_batch(self):
        return dict(itertools.islice(self.dict_queue.items(), 0, self.transaction_size))


if DB_TYPE != 'cumulus':
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
            database = DB
            if DB_TYPE == 'cumulus':
                table_name = 'granules'

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
            if DB_TYPE == 'sqlite':
                conflict_handling = {'action': 'replace'}
            else:
                conflict_handling = {
                    'conflict_target': [Granule.name],
                    'action': 'update',
                    'update': {
                        Granule.discovered_date: datetime.datetime.now(),
                        Granule.status: 'discovered',
                        Granule.etag: EXCLUDED.etag,
                        Granule.last_modified: EXCLUDED.last_modified,
                        Granule.size: EXCLUDED.size
                    }
                }
            return self.__insert_many(granule_dict, conflict_handling)

        def db_error(self, granule_dict, **kwargs):
            """
            Tries to insert all the granules in the granule_dict erroring if there are duplicates
            :param granule_dict: Dictionary containing granules
            """
            return self.__insert_many(granule_dict, {'action': 'rollback'})

        def read_batch(self, collection_id, provider_path, batch_size=1000, **kwargs):
            """
            Fetches N files for up to batch_size granules for the provided collection_id and if the provider path
            is present in the full path of the file.
            :param collection_id: The id of the collection to fetch files for
            :param provider_path: The location where the granule files were discovered from
            :param batch_size: The limit for the number of unique granules to fetch files for
            :return: Returns a list of records that had the status set from "discovered" to queued
            """
            sub_query = (
                self.select(Granule.granule_id).where(
                    (Granule.status == 'discovered') &
                    (Granule.collection_id == collection_id) &
                    (Granule.name.contains(provider_path))
                ).order_by(Granule.discovered_date.asc()).limit(batch_size)
            )

            update = (self.update(status='queued').dicts().where(Granule.granule_id.in_(sub_query)).returning(Granule))
            updated_records = list(update.execute())
            print(f'records returned by query: {len(updated_records)}')

            conversion_dict = {}
            while len(updated_records) > 0:
                record_dict = updated_records[0]
                conversion_dict.update({record_dict.pop('name'): record_dict})
                del updated_records[0]

            updated_records = conversion_dict

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

        @staticmethod
        def data_generator(granule_dict):
            """
            Generator for query tuples
            :param granule_dict: Discover granules dictionary to insert into the database
            :yield: Insertable tuple
            """
            for k, v in granule_dict.items():
                yield k, v['ETag'], v['GranuleId'], v['CollectionId'], 'discovered', v['Last-Modified'], v['Size']

        def query_chunker(self, granule_dict, var_limit):
            """
            Breaks up the queries into subsets that will not overwhelm the query var limit
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
            print(f'Inserting {len(granule_dict)} records...')
            records_inserted = 0
            fields = [Granule.name, Granule.etag, Granule.granule_id, Granule.collection_id, Granule.status,
                      Granule.last_modified, Granule.size]

            var_limit = VAR_LIMIT // len(fields)
            db_st = time.time()
            with DB.atomic():
                for batch in self.query_chunker(granule_dict, var_limit):
                    num = self.insert_many(batch, fields=fields).on_conflict(**conflict_resolution).execute()
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
