import os
from peewee import CharField, Model, chunked, PostgresqlDatabase, ModelTupleCursorWrapper
from playhouse.apsw_ext import APSWDatabase

SQLITE_VAR_LIMIT = 999


class Granule(object):
    """
    Model representing a granule and the associated metadata
    """
    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        self.__db_args = {
            'dbname': database,
            'user': user,
            'password': password,
            'host': host,
            'port': port

        }
        self.__initialize_db()

    def __initialize_db(self):
        """
        Function used to correctly initialize the database.
        If all of the parameters are no assigned then an inmemory SQLite database will be used
        """
        granule_db = PostgresqlDatabase(None) if os.environ.get('RDS_CREDENTIALS_SECRET_ARN') else APSWDatabase(
            ':memory:')

        class CumulusGranule(Model):
            name = CharField(primary_key=True)
            etag = CharField()
            last_modified = CharField()

            class Meta:
                database = granule_db

        self.__RDGGranule = CumulusGranule
        self.__granule_db = granule_db

        if all(self.__db_args.values()):
            granule_db.init(**self.__db_args)
            granule_db.connect()
        else:
            granule_db.init(':memory:')
        granule_db.create_tables([self.__RDGGranule], safe=True)

    def select_all(self, granule_dict):
        """
        Selects all records from the database that are an exact match for all three fields
        :param granule_dict: Dictionary containing granules.
        :return ret_lst: List of granule names that existed in the database
        """
        ret_lst = []
        fields = [self.__RDGGranule.name, self.__RDGGranule.etag, self.__RDGGranule.last_modified]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            etags = set()
            last_mods = set()

            for key in key_batch:
                names.add(key)
                etags.add(granule_dict[key]["ETag"])
                last_mods.add(granule_dict[key]["Last-Modified"])

            sub = self.__RDGGranule\
                .select(self.__RDGGranule.name)\
                .where(self.__RDGGranule.name.in_(names) & self.__RDGGranule.etag.in_(etags) & self.__RDGGranule.last_modified.in_(last_mods))
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
        return self.__insert_many(granule_dict)

    def db_replace(self, granule_dict):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        :param granule_dict: Dictionary containing granules.
        """
        return self.__insert_many(granule_dict)

    def db_error(self, granule_dict):
        """
        Tries to insert all the granules in the granule_dict erroring if there are duplicates
        :param granule_dict: Dictionary containing granules
        """
        fields = [self.__RDGGranule.name]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            for key in key_batch:
                names.add(key)
            res = self.__RDGGranule.select(self.__RDGGranule.name).where(self.__RDGGranule.name.in_(names))
            if res:
                raise ValueError('Granule already exists in the database.')

        return self.__insert_many(granule_dict)

    def delete_granules_by_names(self, granule_names):
        """
        Removes all granule records from the database if the name is found in granule_names.
        :return del_count: The number of deleted granules
        """
        del_count = 0
        for key_batch in chunked(granule_names, SQLITE_VAR_LIMIT):
            delete = self.__RDGGranule.delete().where(self.__RDGGranule.name.in_(key_batch)).execute()
            del_count += delete
        return del_count

    def __insert_many(self, granule_dict):
        """
        Helper function to separate the insert many logic that is reused between queries. Note, that this query assumes
        that the results to be inserted have already been checked against the database. Duplicate values will be
        overwritten.
        :param granule_dict: Dictionary containing granules
        """
        records_inserted = 0
        data = [(k, v['ETag'], v['Last-Modified']) for k, v in granule_dict.items()]
        fields = [self.__RDGGranule.name, self.__RDGGranule.etag, self.__RDGGranule.last_modified]
        with self.__granule_db.atomic():
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                num = self.__RDGGranule.insert_many(key_batch, fields=[self.__RDGGranule.name, self.__RDGGranule.etag, self.__RDGGranule.last_modified])\
                    .on_conflict(
                    conflict_target=[self.__RDGGranule.name],
                    preserve=[self.__RDGGranule.etag, self.__RDGGranule.last_modified]
                ).execute()

                # Note: The result of the query is a different type between postgres and sqlite so the following check
                # is needed.
                if isinstance(num, ModelTupleCursorWrapper):
                    records_inserted += num.count
                else:
                    records_inserted += num

        return records_inserted


if __name__ == '__main__':
    pass
