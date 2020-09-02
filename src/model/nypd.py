from collections import namedtuple


_nypd_tuple = namedtuple('Nypd', [
    'id',
    'date',
    'time',
    'precinct',
    'offense_code',
    'offense_description',
    'crime_outcome',
    'level_offense',
    'borough',
    'latitude',
    'longitude',
    'age',
    'race',
    'sex'
])


class Nypd(_nypd_tuple):

    def __repr__(self):
        nypd = ''
        for k, v in self._asdict().items():
            nypd += f'{k}: {v}\n'
        return nypd

    def __str__(self):
        nypd = ''
        for k, v in self._asdict().items():
            nypd += f'{k}: {v}\n'
        return nypd

    def to_dict(self):
        return dict(self._asdict())