import os
import string

from peewee import *
from playhouse.sqlite_ext import SqliteExtDatabase


SQLITE_VAR_LIMIT = 999
dgm_db_file_name = ''
db = SqliteExtDatabase(None)
lock_file = f'{os.getenv("efs_path", "tmp")}/discover_granules.lock'
RANG_STR = string.ascii_uppercase


def initialize_db(db_file_path=dgm_db_file_name):
    global db
    db.init(db_file_path, pragmas={
        'journal_mode': 'wal',
        'cache_size': -1 * 64000,  # 64MB
        'foreign_keys': 1})
    db.create_tables([Granule], safe=True)

    return Granule()


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

            # sub = Granule.raw(f'SELECT name FROM granule'
            #                   f' WHERE name IN {names} AND etag IN {etags} AND last_modified IN {last_mods}')
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
        fields = [Granule.name]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = ''
            for key in key_batch:
                names = f'{names}\'{key}\','
            names = f'({names.rstrip(",")})'
            # res = Granule.raw(f'SELECT name FROM granule WHERE name IN {names}')
            res = Granule.select(Granule.name).where(Granule.name.in_(names))
            if res:
                raise ValueError('Granule already exists in the database.')

        self.__insert_many(granule_dict)

    @staticmethod
    def delete_granules_by_names(granule_names):
        del_count = 0
        for key_batch in chunked(granule_names, SQLITE_VAR_LIMIT):
            d = Granule.delete().where(Granule.name.in_(key_batch)).execute()
            del_count += d
        return del_count

    @staticmethod
    def __insert_many(granule_dict):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param granule_dict: Dictionary containing granules
        """
        data = [(k, v['ETag'], v['Last-Modified']) for k, v in granule_dict.items()]
        fields = [Granule.name, Granule.etag, Granule.last_modified]
        with db.atomic():
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                s = Granule.insert_many(key_batch, fields=[Granule.name, Granule.etag, Granule.last_modified]) \
                    .on_conflict_replace().execute()


if __name__ == '__main__':
    pass
