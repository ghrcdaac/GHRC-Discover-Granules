import json
import os
import time

import boto3
import psycopg2
from psycopg2 import sql

from task.dbm_base import DBManagerBase

VAR_LIMIT = 32766


def get_db_manager_cumulus(**kwargs):
    return DBManagerCumulus(**kwargs)


class DBManagerCumulus(DBManagerBase):
    def __init__(self, collection_id, database, **kwargs):
        super().__init__(**kwargs)
        self.collection_id = collection_id

        if database:
            self.DB = database
        else:
            sm = boto3.client('secretsmanager')
            secrets_arn = os.getenv('cumulus_credentials_arn', None)
            print(f'arn: {secrets_arn}')
            db_init_kwargs = json.loads(sm.get_secret_value(SecretId=secrets_arn).get('SecretString'))
            db_init_kwargs.update({'user': db_init_kwargs.pop('username')})
            db_init_kwargs.update({'connect_timeout ': 30})
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
            while index < len(self.list_dict):
                granule_id = self.list_dict[index].get('granule_id')
                if granule_id in db_granule_ids:
                    del self.list_dict[index]
                else:
                    index += 1

        self.discovered_files_count += len(self.list_dict)

    def read_batch(self):
        self.queued_files_count += len(self.list_dict)
        return self.list_dict

    def trim_results(self):
        granule_ids = [x.get('granule_id') for x in self.list_dict]
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

    def filter_against_cumulus(self, granule_list_dict):
        discovered_granule_ids = []
        for x in granule_list_dict:
            discovered_granule_ids.append(x.get('granule_id'))
            discovered_granule_ids.append(x.get('last_modified'))

        query_params_tuple = tuple(discovered_granule_ids)
        print(f'checking cumulus for : {len(granule_list_dict)} granule IDs...')
        results = []
        values_string = ','.join('(%s,%s)' for x in range(len(granule_list_dict)))
        with self.DB:
            with self.DB.cursor() as curs:
                query_string = sql.SQL(
                    f'WITH discovered(granule_id, last_modified) AS (VALUES {values_string}), '
                    'extant_ids AS ('
                    'SELECT granules.granule_id, granules.timestamp, discovered.last_modified '
                    'FROM granules '
                    'JOIN discovered ON granules.granule_id=discovered.granule_id'
                    ') '
                    'SELECT granule_id FROM extant_ids '
                    'WHERE last_modified::timestamp < timestamp::timestamp '
                )
                # print(f'Trim query: {query_string}')
                # print(f'Trim query: {curs.mogrify(query_string, query_params_tuple)}')
                curs.execute(query_string, query_params_tuple)
                results.extend([x[0] for x in curs.fetchall()])

        result_set = set(results)
        print(f'granule IDs extant in cumulus: {len(result_set)}')
        return result_set


if __name__ == '__main__':
    pass
