#!/usr/bin/env python
"""mapper.py"""

import sys

from config import SEPARATOR, POP, RACE, INDEX_POP, INDEX_RACE


class Mapper:
    def __init__(self):
        pass

    def read_db(self):
        for line in sys.stdin:
            yield line

    def run(self):
        doc = self.read_db()
        # fields = next(doc).rstrip('\n').split(',')
        index_pop = INDEX_POP # fields.index(POP)
        index_race = INDEX_RACE # fields.index(RACE)
        for d in doc:
            d = d.rstrip('\n').split(',')
            if d[index_pop] != POP and d[index_race] != RACE:
                race = d[index_race].split()
                print(f'{self._convert(race)}{SEPARATOR}{d[index_pop]}')

    def _convert(self, doc):
        race = {
            'American': 'AMERICAN INDIAN/ALASKAN NATIVE',
            'Asian': 'ASIAN / PACIFIC ISLANDER',
            'Native': 'ASIAN / PACIFIC ISLANDER'
        }.get(doc[0])

        if doc[0] == 'Black':
            race = 'BLACK HISPANIC' if doc[-1] == 'Hispanic' else 'BLACK'

        if doc[0] == 'White':
            race = 'WHITE HISPANIC' if doc[-1] == 'Hispanic' else 'WHITE'

        return race


if __name__ == '__main__':
    mapper = Mapper()
    mapper.run()
