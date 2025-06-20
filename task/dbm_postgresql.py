import datetime
import json
import os
import time

import boto3
from playhouse.postgres_ext import PostgresqlExtDatabase, Model, CharField, DateTimeField, EXCLUDED, chunked,\
    BigIntegerField
from psycopg2 import sql

from task.dbm_base import DBManagerPeewee, TABLE_NAME, get_db_params

DB_PSQL = PostgresqlExtDatabase(None)
VAR_LIMIT_PSQL = 32766


def get_db_manager_psql(database, **kwargs):
    global DB_PSQL # noqa: F824
    db_init_kwargs = {}
    if database:
        db_init_kwargs = get_db_params(kwargs)
        db_init_kwargs.update({'database': database})
    else:
        secrets_arn = os.getenv('postgresql_secret_arn', None)
        sm_client = boto3.client('secretsmanager')
        secrets = json.loads(sm_client.get_secret_value(SecretId=secrets_arn).get('SecretString'))
        db_init_kwargs = get_db_params(secrets)

    DB_PSQL.init(**db_init_kwargs)
    DB_PSQL.create_tables([GranulePSQL], safe=True)

    return DBManagerPSQL(DB_PSQL, GranulePSQL, **kwargs)


class GranulePSQL(Model):
    name = CharField(primary_key=True)
    granule_id = CharField()
    collection_id = CharField()
    status = CharField(default='discovered')
    etag = CharField()
    last_modified = CharField()
    discovered_date = DateTimeField(formats='YYYY-mm-dd HH:MM:SS', default=datetime.datetime.now)
    size = BigIntegerField()

    class Meta:
        database = DB_PSQL
        table_name = TABLE_NAME


class DBManagerPSQL(DBManagerPeewee):
    def __init__(self, database, model_class, **kwargs):
        self.model_class = model_class
        super().__init__(database, model_class, VAR_LIMIT_PSQL, EXCLUDED, chunked, **kwargs)

    def db_replace(self):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        """
        conflict_handling = {
            'conflict_target': [self.model_class.name],
            'action': 'update',
            'update': {
                self.model_class.discovered_date: datetime.datetime.now(),
                self.model_class.status: 'discovered',
                self.model_class.etag: self.excluded.etag,
                self.model_class.last_modified: self.excluded.last_modified,
                self.model_class.size: self.excluded.size
            }
        }
        return self.insert_many(conflict_handling)

    def ignore_discovered(self):
        """
        Will change the status to ignored for a given collection_id and provider prefix: protocol://host/path/to/granules/
        """
        ignore_query = sql.SQL(
            """
            WITH update_rows AS (
            SELECT name
            FROM granule
            WHERE granule.name LIKE (%s) AND
                  granule.collection_id = (%s) AND
                  granule.status = 'discovered'
            FOR UPDATE OF granule
            )
            UPDATE granule
            SET status = 'ignored'
            FROM update_rows
            WHERE update_rows.name = granule.name
            """
        )
        with self.database.cursor() as cur:
            cur.execute(ignore_query, [f'{self.provider_full_url}%', self.collection_id])
            ignore_count = cur.rowcount
        print(f'Set status for {ignore_count} records to "ignored"')

    def read_batch(self):
        repeat_args = [f'{self.provider_full_url}%', self.collection_id]
        query_args = repeat_args + [self.file_count, self.batch_limit] + repeat_args

        update_query = sql.SQL(
            """
            WITH granule_ids AS (
            SELECT granule_id
            FROM granule
            WHERE granule.name LIKE (%s) AND
                granule.collection_id = (%s) AND
                granule.status = 'discovered'
            GROUP BY granule_id
            HAVING COUNT(granule_id) >= (%s)
            ORDER BY MIN(discovered_date)
            LIMIT (%s)
            ),
            rows AS (
            SELECT name
            FROM granule, granule_ids
            WHERE granule.name LIKE (%s) AND
                granule.collection_id = (%s) AND
                granule.status = 'discovered' AND
                granule.granule_id = granule_ids.granule_id
            FOR UPDATE OF granule
            )
            UPDATE granule
            SET status = 'queued'
            FROM rows
            WHERE rows.name = granule.name
            RETURNING granule.*
            """
        )

        st = time.time()
        with self.database.cursor() as cur:
            cur.execute(update_query, query_args)
            # print(cur.mogrify(update_query, query_args).decode().replace('\n', '\r').strip()) # Uncomment when troubleshooting queries
            res = cur.fetchall()

        self.database.commit()
        td = []
        column_names = [
            'name', 'granule_id', 'collection_id', 'status', 'etag', 'last_modified', 'discovered_date', 'size'
        ]
        for row in res:
            temp_dict = {}
            for column_name, value  in zip(column_names, row):
                temp_dict.update({column_name: value})
            td.append(temp_dict)

        et = time.time() - st
        print(f'Updated {len(td)} records in {et} seconds.')
        print(f'Rate: {int(len(td) / et)}/s')

        self.queued_files_count += len(td)
        return td

    @staticmethod
    def add_for_update(select_query):
        """
        Add the FOR UPDATE clause to PSQL queries to ensure the rows being updates cannot be updated by another
        connected client.
        :param select_query: The subquery for an update query.
        :return: The select query with the added FOR UPDATE clause
        """
        return select_query.for_update(for_update=True)


if __name__ == '__main__':
    pass
