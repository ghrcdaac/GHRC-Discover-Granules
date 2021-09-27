from peewee import *


SQLITE_VAR_LIMIT = 999
DB_FILENAME = 'discover_granules.db'
# Note: Lambda execution requires the db file to be in /tmp
db = SqliteDatabase(f'/tmp/{DB_FILENAME}')


class Granule(Model):
    """
    Model representing a granule and the associated metadata
    """
    name = CharField(primary_key=True)
    etag = CharField()
    last_modified = CharField()

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
    def __insert_many(granule_dict):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param granule_dict: Dictionary containing granules
        """
        data = [(k, v['ETag'], v['Last-Modified']) for k, v in granule_dict.items()]
        with db.atomic():
            fields = [Granule.name, Granule.etag, Granule.last_modified]
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                Granule.insert_many(key_batch, fields=[Granule.name, Granule.etag, Granule.last_modified]) \
                    .on_conflict_replace().execute()

    class Meta:
        database = db


if __name__ == '__main__':
    pass
