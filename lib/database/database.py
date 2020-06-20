from .ops_nypd import NypdOpsMixin


class _DbBase(object):
    def __init__(self, *args, **kwargs):
        pass


class Database(
        NypdOpsMixin,
        _DbBase):

    def __init__(self, db_connection):
        super().__init__(db_connection)