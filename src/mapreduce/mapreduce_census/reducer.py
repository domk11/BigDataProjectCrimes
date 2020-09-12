#!/usr/bin/env python
"""reducer.py"""

import sys
from itertools import groupby
from operator import itemgetter

from config import SEPARATOR


class Reducer:
    def __init__(self):
        pass

    def read_mapper_output(self):
        for record in sys.stdin:
            yield record.rstrip().split(SEPARATOR, 1)

    def run(self):
        data = self.read_mapper_output()
        for current_word, group in groupby(data, itemgetter(0)):
            try:
                total_count = sum(int(count) for current_word, count in group)
                print(f'{current_word}{SEPARATOR}{total_count}')
            except ValueError:
                pass


if __name__ == '__main__':
    reducer = Reducer()
    reducer.run()
