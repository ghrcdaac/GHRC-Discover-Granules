import logging
import os
import time
from time import sleep

import boto3
from cumulus_logger import CumulusLogger

import botocore.exceptions
from peewee import *

logging_level = logging.INFO if os.getenv('enable_logging', 'false').lower() == 'true' else logging.WARNING
logger = CumulusLogger(name='Recursive-Discover-Granules', level=logging_level)

SQLITE_VAR_LIMIT = 999
DB_FILENAME = 'discover_granules.db'
DB_FILE_PATH = f'/tmp/{DB_FILENAME}'
# Note: Lambda execution requires the db file to be in /tmp
db = SqliteDatabase(DB_FILE_PATH)


class Granule(Model):
    """
    Model representing a granule and the associated metadata
    """
    name = CharField(primary_key=True)
    etag = CharField()
    last_modified = CharField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_client = boto3.client('s3')

        self.db_key = f'{os.getenv("s3_key_prefix", default="temp").rstrip("/")}/{DB_FILENAME}'
        self.s3_bucket_name = os.getenv('bucket_name')

        table_resource = boto3.resource('dynamodb', region_name='us-west-2')
        self.db_table = table_resource.Table(os.getenv('table_name', default='DiscoverGranulesLock'))

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
            for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
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
            for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
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
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                s = Granule.insert_many(key_batch, fields=[Granule.name, Granule.etag, Granule.last_modified])\
                    .on_conflict_replace().execute()

    def lock_db(self):
        """
        This function attempts to create or update the database lock entry in dynamodb. If the entry already exists it
        will attempt to create it for 14 minutes and 5 seconds. Once the entry is created it will break from the loop. A
        5 second window is left at the end so that the ValueError can be thrown in the event that the lock doesn't
        expire in time.
        """
        timeout = 895
        while timeout:
            try:
                current_time = int(time.time())
                lock_expiration = current_time + 300
                self.db_table.update_item(
                    Key={
                        'DatabaseLocked': 'locked'
                    },
                    UpdateExpression=f'SET LockExpirationEpoch = :lock_expiration',
                    ConditionExpression='(attribute_not_exists(DatabaseLocked)) OR'
                                        ' (LockExpirationEpoch <= :current_time)',
                    ExpressionAttributeValues={':current_time': current_time, ':lock_expiration': lock_expiration}
                )
                break
            except self.db_table.meta.client.exceptions.ConditionalCheckFailedException:
                logger.info('Waiting on lock...')
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

    def read_db_file(self):
        """
        Reads the SQLite database file from S3
        """
        self.lock_db()
        try:
            self.s3_client.download_file(self.s3_bucket_name, self.db_key, DB_FILE_PATH)
        except botocore.exceptions.ClientError as err:
            # The db files doesn't exist in S3 yet so create it
            db.connect()
            db.create_tables([Granule])

    def write_db_file(self):
        """
        Writes the SQLite database file to S3.
        """
        db.close()
        self.s3_client.upload_file(DB_FILE_PATH, self.s3_bucket_name, self.db_key)
        self.unlock_db()

    @staticmethod
    def db_file_cleanup():
        """
        This function deletes the database file stored in the lambda as each invocation can be using a previously used
        file system with the old db file.
        """
        os.remove(DB_FILE_PATH)

    class Meta:
        database = db


if __name__ == '__main__':
    pass
