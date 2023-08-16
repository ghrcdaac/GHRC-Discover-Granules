import datetime
import json
import os
import time

import boto3
from playhouse.postgres_ext import PostgresqlExtDatabase, Model, CharField, DateTimeField, IntegerField, EXCLUDED, \
    chunked

from task.dbm_base import DBManagerPeewee, TABLE_NAME

DB_PSQL = PostgresqlExtDatabase(None)
VAR_LIMIT_PSQL = 32766


def get_db_manager_psql(collection_id, provider_full_url, database, duplicate_handling, batch_limit, transaction_size,
                        cumulus_filter=None, auto_batching=True):
    global DB_PSQL
    db_init_kwargs = {}
    if database:
        DB_PSQL = database
    else:
        sm = boto3.client('secretsmanager')
        secrets_arn = os.getenv('postgresql_secret_arn', None)
        secrets = sm.get_secret_value(SecretId=secrets_arn).get('SecretString')
        db_init_kwargs = json.loads(secrets)

    DB_PSQL.init(**db_init_kwargs)
    DB_PSQL.create_tables([GranulePSQL], safe=True)

    return DBManagerPSQL(
        collection_id, provider_full_url, GranulePSQL, DB_PSQL, auto_batching, batch_limit, transaction_size,
        duplicate_handling, cumulus_filter
    )


class GranulePSQL(Model):
    name = CharField(primary_key=True)
    granule_id = CharField()
    collection_id = CharField()
    status = CharField(default='discovered')
    etag = CharField()
    last_modified = CharField()
    discovered_date = DateTimeField(formats='YYYY-mm-dd HH:MM:SS', default=datetime.datetime.now)
    size = IntegerField()

    class Meta:
        database = DB_PSQL
        table_name = TABLE_NAME


class DBManagerPSQL(DBManagerPeewee):
    def __init__(
            self, collection_id, provider_full_url, model_class, database, auto_batching, batch_limit, transaction_size,
            duplicate_handling, cumulus_filter
    ):
        self.model_class = model_class
        super().__init__(
            collection_id, provider_full_url, self.model_class, database, auto_batching, batch_limit, transaction_size,
            duplicate_handling, cumulus_filter, VAR_LIMIT_PSQL, EXCLUDED, chunked
        )

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

    def read_batch(self):
        st = time.time()
        sub_query = "SELECT granule.granule_id FROM granule WHERE (granule.status = (%s) AND granule.collection_id = (%s) AND granule.name LIKE (%s)) ORDER BY granule.discovered_date LIMIT (%s) FOR UPDATE"
        update_query = f"UPDATE granule SET status = 'queued' WHERE granule_id in ({sub_query}) RETURNING *"
        print(f'query: {update_query}')
        with self.database.cursor() as cur:
            cur.execute(update_query, ('discovered', self.collection_id, f'{self.provider_full_url}%%', self.batch_limit))
            res = cur.fetchall()

        self.database.commit()
        td = []
        column_names = [
            'name', 'granule_id', 'collection_id', 'status', 'etag', 'last_modified', 'discovered_date', 'size'
        ]
        for x in res:
            temp_dict = {}
            for value, column_name in zip(x, column_names):
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
