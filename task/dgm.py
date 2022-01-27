import logging
import os
import time
from time import sleep

import boto3
from cumulus_logger import CumulusLogger

from peewee import *

logging_level = logging.INFO if os.getenv('enable_logging', 'false').lower() == 'true' else logging.WARNING
logger = CumulusLogger(name='Recursive-Discover-Granules', level=logging_level)


def make_model(db_suffix):
    sqlite_var_limit = 999
    db_filename = f'discover_granules_{db_suffix}.db'
    db_file_path = f'/{os.getenv("efs_path", "tmp")}/{db_filename}'
    db = SqliteDatabase(db_file_path)

    class Granule(Model):
        """
        Model representing a granule and the associated metadata
        """
        name = CharField(primary_key=True)
        etag = CharField()
        last_modified = CharField()

        class Meta:
            database = db

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.s3_client = boto3.client('s3')

            self.db_key = f'{os.getenv("s3_key_prefix", default="temp").rstrip("/")}/{db_filename}'
            self.s3_bucket_name = os.getenv('bucket_name')

            table_resource = boto3.resource('dynamodb', region_name='us-west-2')
            self.db_table = table_resource.Table(os.getenv('table_name', default='DiscoverGranulesLock'))
            pass

        @staticmethod
        def select_all(granule_dict):
            """
            Selects all records from the database that are an exact match for all three fields
            :param granule_dict: Dictionary containing granules.
            :return ret_lst: List of granule names that existed in the database
            """
            ret_lst = []
            with db.atomic():
                fields = [Granule.name, Granule.etag, Granule.last_modified]
                for key_batch in chunked(granule_dict, sqlite_var_limit // len(fields)):
                    etags = ''
                    last_mods = ''
                    names = ''
                    for key in key_batch:
                        names = f'{names}\'{key}\','
                        etags = f'{etags}\'{granule_dict[key]["ETag"]}\','
                        last_mods = f'{last_mods}\'{granule_dict[key]["Last-Modified"]}\','

                    etags = f'({etags.rstrip(",")})'
                    last_mods = f'({last_mods.rstrip(",")})'
                    names = f'({names.rstrip(",")})'

                    sub = Granule.raw(f'SELECT name FROM granule'
                                      f' WHERE name IN {names} AND etag IN {etags} AND last_modified IN {last_mods}')
                    for name in sub.tuples().iterator():
                        ret_lst.append(name[0])

            return ret_lst

        def db_skip(self, granule_dict):
            """
            Inserts all the granules in the granule_dict unless they already exist
            :param granule_dict: Dictionary containing granules.
            """
            for name in self.select_all(granule_dict):
                granule_dict.pop(name)
            self.__insert_many(granule_dict)

        def db_replace(self, granule_dict):
            """
            Inserts all the granules in the granule_dict overwriting duplicates if they exist
            :param granule_dict: Dictionary containing granules.
            """
            self.__insert_many(granule_dict)

        def db_error(self, granule_dict):
            """
            Tries to insert all the granules in the granule_dict erroring if there are duplicates
            :param granule_dict: Dictionary containing granules
            """
            with db.atomic():
                fields = [Granule.name]
                for key_batch in chunked(granule_dict, sqlite_var_limit // len(fields)):
                    names = ''
                    for key in key_batch:
                        names = f'{names}\'{key}\','
                    names = f'({names.rstrip(",")})'
                    res = Granule.raw(f'SELECT name FROM granule WHERE name IN {names}')
                    if res:
                        raise ValueError('Granule already exists in the database.')

            self.__insert_many(granule_dict)

        @staticmethod
        def remove_granules_by_name(granule_names):
            del_count = 0
            for name in granule_names:
                d = Granule.delete().where(Granule.name.endswith(name)).execute()
                del_count += d
                logger.info(f'Deleted {d} record with suffix {name}')

            return del_count

        @staticmethod
        def __insert_many(granule_dict):
            """
            Helper function to separate the insert many logic that is reused between queries
            :param granule_dict: Dictionary containing granules
            """
            data = [(k, v['ETag'], v['Last-Modified']) for k, v in granule_dict.items()]
            with db.atomic():
                fields = [Granule.name, Granule.etag, Granule.last_modified]
                for key_batch in chunked(data, sqlite_var_limit // len(fields)):
                    s = Granule.insert_many(key_batch, fields=[Granule.name, Granule.etag, Granule.last_modified])\
                        .on_conflict_replace().execute()

        def lock_db(self):
            """
            This function attempts to create a database lock entry in dynamodb. If the entry already exists it will attempt
            to create it for five minutes while also calling db_lock_mitigation. Once the entry is created it will break
            from the loop.
            """
            timeout = 600
            while timeout:
                try:
                    self.db_table.put_item(
                        Item={
                            'DatabaseLocked': 'locked',
                            'LockDuration': str(time.time() + 900)
                        },
                        ConditionExpression='attribute_not_exists(DatabaseLocked)'
                    )
                    break
                except self.db_table.meta.client.exceptions.ConditionalCheckFailedException:
                    logger.info('waiting on lock.')
                    timeout -= 1
                    sleep(1)

            if not timeout:
                raise ValueError('Timeout: Unsuccessful in creating database lock.')

        def unlock_db(self):
            """
            Used to delete the "lock" entry in the dynamodb table.
            """
            self.db_table.delete_item(
                Key={
                    'DatabaseLocked': 'locked'
                }
            )

    return Granule


if __name__ == '__main__':
    pass
