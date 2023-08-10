import datetime
import json
import os

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

    @staticmethod
    def add_for_update(select_query):
        """
        Add the FOR UPDATE clause to PSQL queries to ensure the rows being updates cannot be updated by another
        connected client.
        :param select_query: The subquery for an update query.
        :return: The select query with the added FOR UPDATE clause
        """
        return select_query.for_update()


if __name__ == '__main__':
    pass
