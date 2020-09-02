from src.database.contracts import nypd_contract as c


class Filter:
    def __init__(self, rdd):
        self.rdd = rdd

    def filter_sex(self, sex):
        return self.rdd.filter(lambda x: (x[c.SEX] == sex))

    def filter_age(self, age):
        return self.rdd.filter(lambda x: (x[c.AGE] == f'<{age}'))