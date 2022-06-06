import os

from peewee import CharField, Model, chunked, PostgresqlDatabase, ModelTupleCursorWrapper
from playhouse.apsw_ext import APSWDatabase

SQLITE_VAR_LIMIT = 999
db = PostgresqlDatabase(None) if os.environ.get('RDS_CREDENTIALS_SECRET_ARN') else APSWDatabase(':memory:')


def initialize_db(dbname=None, user=None, password=None, host=None, port=None):
    global db
    if dbname and user and password and host and port:
        db.init(database=dbname, user=user, password=password, host=host, port=port)
        db.connect()
        db.create_tables([Granule], safe=True)
    else:
        db.init(':memory:')
        db.create_tables([Granule], safe=True)


class Granule(Model):
    """
    Model representing a granule and the associated metadata
    """
    name = CharField(primary_key=True)
    etag = CharField()
    last_modified = CharField()

    class Meta:
        database = db

    @staticmethod
    def select_all(granule_dict):
        """
        Selects all records from the database that are an exact match for all three fields
        :param granule_dict: Dictionary containing granules.
        :return ret_lst: List of granule names that existed in the database
        """
        ret_lst = []
        fields = [Granule.name, Granule.etag, Granule.last_modified]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            etags = set()
            last_mods = set()

            for key in key_batch:
                names.add(key)
                etags.add(granule_dict[key]["ETag"])
                last_mods.add(granule_dict[key]["Last-Modified"])

            sub = Granule\
                .select(Granule.name)\
                .where(Granule.name.in_(names) & Granule.etag.in_(etags) & Granule.last_modified.in_(last_mods))
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
        fields = [Granule.name]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            for key in key_batch:
                names.add(key)
            res = Granule.select(Granule.name).where(Granule.name.in_(names))
            if res:
                raise ValueError('Granule already exists in the database.')

        return self.__insert_many(granule_dict)

    @staticmethod
    def delete_granules_by_names(granule_names):
        """
        Removes all granule records from the database if the name is found in granule_names.
        :return del_count: The number of deleted granules
        """
        del_count = 0
        for key_batch in chunked(granule_names, SQLITE_VAR_LIMIT):
            delete = Granule.delete().where(Granule.name.in_(key_batch)).execute()
            del_count += delete
        return del_count

    @staticmethod
    def __insert_many(granule_dict):
        """
        Helper function to separate the insert many logic that is reused between queries. Note, that this query assumes
        that the results to be inserted have already been checked against the database. Duplicate values will be
        overwritten.
        :param granule_dict: Dictionary containing granules
        """
        records_inserted = 0
        data = [(k, v['ETag'], v['Last-Modified']) for k, v in granule_dict.items()]
        fields = [Granule.name, Granule.etag, Granule.last_modified]
        with db.atomic():
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                num = Granule.insert_many(key_batch, fields=[Granule.name, Granule.etag, Granule.last_modified])\
                    .on_conflict(
                    conflict_target=[Granule.name],
                    preserve=[Granule.etag, Granule.last_modified]
                ).execute()

                # Note: The result of the query is a different type between postgres and sqlite so the following check
                # is needed.
                if type(num) is ModelTupleCursorWrapper:
                    records_inserted += num.count
                else:
                    records_inserted += num

        return records_inserted


if __name__ == '__main__':
    pass
