import json
import os
import time

import boto3
import psycopg2

from task.dbm_base import DBManagerBase

VAR_LIMIT = 32766


def get_db_manager_cumulus(database, duplicate_handling, batch_limit, transaction_size):
    return DBManagerCumulus(database, duplicate_handling, batch_limit, transaction_size)


class DBManagerCumulus(DBManagerBase):
    def __init__(self, database, duplicate_handling, batch_limit, transaction_size):
        super().__init__(duplicate_handling, batch_limit, transaction_size)
        if database:
            self.DB = database
        else:
            sm = boto3.client('secretsmanager')
            secrets_arn = os.getenv('cumulus_credentials_arn', None)
            db_init_kwargs = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))
            db_init_kwargs.update({'user': db_init_kwargs.pop('username')})
            self.DB = psycopg2.connect(**db_init_kwargs) if 'psycopg2' in globals() else None

    def close_db(self):
        self.DB.close()

    def flush_dict(self):
        if self.duplicate_handling == 'skip':
            db_granule_ids = self.trim_results()
            db_granule_ids = set(db_granule_ids)
            print(f'Trimming {len(db_granule_ids)} files that already existed in the Cumulus database.')
            print(f'Trimmed granule IDs: {db_granule_ids}')

            # Remove the keys that have already been discovered
            index = 0
            while index < len(self.dict_list):
                granule_id = self.dict_list[index].get('granule_id')
                if granule_id in db_granule_ids:
                    del self.dict_list[index]
                else:
                    index += 1

        self.discovered_files_count += len(self.dict_list)

    def read_batch(self):
        self.queued_files_count += len(self.dict_list)
        return self.dict_list

    def trim_results(self):
        granule_ids = [x.get('granule_id') for x in self.dict_list]
        print(f'granule_ids: {granule_ids}')
        results = []
        start_index = 0
        end_index = VAR_LIMIT
        db_st = time.time()
        while True:
            with self.DB:
                with self.DB.cursor() as curs:
                    id_batch = tuple(granule_ids[start_index:end_index])
                    if len(id_batch) == 0:
                        break
                    print(f'id_batch" {id_batch}')
                    query_string = 'SELECT granules.granule_id FROM granules WHERE granules.granule_id IN %s;'
                    print(f'Trim query: {query_string}')
                    curs.execute(query_string, (id_batch,))
                    results.extend([x[0] for x in curs.fetchall()])
                    start_index = end_index + 1
                    end_index += VAR_LIMIT
        db_et = time.time() - db_st
        print(f'{len(results)} records read in {db_et} seconds')
        print(f'Rate: {int(len(results) / db_et)}/s')

        return results

    def filter_against_cumulus(self, granule_ids_tuple):
        """
        Returns a set of granule_ids that were not in the cumulus database
        """
        print(f'Filtering {len(granule_ids_tuple)} granule IDs against the cumulus database...')
        st = time.time()
        with self.DB.cursor() as curs2:
            query_string_3 = 'SELECT granules.granule_id FROM granules WHERE granules.granule_id IN %s;'
            curs2.execute(query_string_3, (granule_ids_tuple,))
            fetched_ids_set = set(record_tuple[0] for record_tuple in curs2.fetchall())
            count = len(fetched_ids_set)
            print(f'{count} records existed in the database')

        discovered_ids_set = {x for x in granule_ids_tuple}
        new_ids_set = discovered_ids_set.difference(fetched_ids_set)
        print(f'ID count after filtering: {len(new_ids_set)}')

        et = time.time() - st
        print(f'Retrieved Rows: {count}')
        print(f'Duration: {et}')
        print(f'Rate: {count} rows/{et} s')

        return new_ids_set
