# The original dataset can be retrieved from this link
# https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD

from bson import ObjectId

from .contracts import nypd_contract as c
from .codecs import nypd_from_document


def nypd_projection():
    return {
        c.ID: f'${c.ID}',
        c.DATE: f'${c.DATE}',
        c.TIME: f'${c.TIME}',
        c.PRECINCT: f'${c.PRECINCT}',
        c.OFFENSE_CODE: f'${c.OFFENSE_CODE}',
        c.OFFENSE_DESCRIPTION: f'${c.OFFENSE_DESCRIPTION }',
        c.CRIME_OUTCOME: f'${c.CRIME_OUTCOME }',
        c.LEVEL_OFFENSE: f'${c.LEVEL_OFFENSE}',
        c.BOROUGH: f'${c.BOROUGH }',
        c.LATITUDE: f'${c.LATITUDE}',
        c.LONGITUDE: f'${c.LONGITUDE}',
        c.AGE: f'${c.AGE}',
        c.RACE: f'${c.RACE}',
        c.SEX: f'${c.SEX}'
    }


def date_regex(start, end):
    return ''.join([f'{str(year)}|' for year in range(start, end)]) + str(end)


class NypdOpsMixin:

    def __init__(self, db_connection):
        self.nypd = db_connection[c.COLLECTION_NAME]
        self.filtered_nypd = db_connection[c.FILTERED_COLLECTION]

    def filter_data(self, limit=0):
        return self.nypd.find({
            c.DATE: {'$regex': date_regex(2009, 2019), '$exists': True, '$ne': ''},
            c.RACE: {'$exists': True, '$ne': ''}
        }, nypd_projection()).limit(limit)

    def put_filtered_data(self, nypd_data):
        return str(self.filtered_nypd.insert_one(nypd_data).inserted_id)

    def get_collection(self):
        nypd_doc = self.filtered_nypd.find()
        return (nypd_from_document(nypd) for nypd in nypd_doc)

    def get_nypd(self, nypd_id):
        nypd_doc = self.filtered_nypd.find_one({c.ID: ObjectId(nypd_id)})
        return nypd_from_document(nypd_doc) if nypd_doc is not None else None

    def get_borough(self, nypd_borough, limit=10):
        nypd_doc = self.filtered_nypd.find(
            {c.BOROUGH: {'$regex': f'{nypd_borough}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]

    def get_race(self, race, limit=10):
        nypd_doc = self.filtered_nypd.find(
            {c.RACE: {'$regex': f'{race}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]

    def get_sex(self, sex, limit=10):
        nypd_doc = self.filtered_nypd.find(
            {c.SEX: {'$regex': f'{sex}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]

    def get_age(self, age, limit=10):
        nypd_doc = self.filtered_nypd.find(
            {c.AGE: {'$regex': f'{age}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]
