#!/usr/bin/env python
"""mapper.py"""

import sys

from config import SEPARATOR, MAPPER_FIELD, NUMBER_OF_FIELDS, INDEX_FIELD


class Mapper:
    def __init__(self):
        pass

    def read_db(self):
        for line in sys.stdin:
            yield line

    def run(self):
        doc = self.read_db()
        # fields = next(doc).split(',')
        length = NUMBER_OF_FIELDS # len(fields)
        index = INDEX_FIELD # fields.index(MAPPER_FIELD)
        for d in doc:
            d = d.split(',')
            if d[index] != MAPPER_FIELD:
                print(f'{d[index]}{SEPARATOR}1' if len(d) == length else f'{d[index + (len(d) - length)]}{SEPARATOR}1')


if __name__ == '__main__':
    mapper = Mapper()
    mapper.run()
