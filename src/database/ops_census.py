# The original dataset can be retrieved from this link
# https://www2.census.gov/programs-surveys/popest/datasets/2010/modified-race-data-2010/stco-mr2010_mt_wy.csv

from .contracts import census_contract as c


def census_projection():
    return {
        c.ID: f'${c.ID}',
        c.STATE: f'${c.STATE}',
        c.COUNTY: f'${c.COUNTY}',
        c.SEX: f'${c.SEX}',
        c.FLAG_HISPANIC: f'${c.FLAG_HISPANIC}',
        c.AGE_RANGE: f'${c.AGE_RANGE}',
        c.RACE: f'${c.RACE}',
        c.POP: f'${c.POP}'
    }


class CensusOpsMixin:

    def __init__(self, db_connection):
        self.us = db_connection[c.COLLECTION_NAME]
        self.ny = db_connection[c.FILTERED_COLLECTION]

    def filter_census_data(self, limit=0):
        return self.us.find({
            c.STATE: {'$regex': 'new york', '$options': '-i', '$exists': True, '$ne': ''},
            c.POP: {'$exists': True, '$ne': 0},
            c.RACE: {'$exists': True, '$ne': ''}
        }, census_projection()).limit(limit)

    def put_filtered_census_data(self, ny_data):
        return str(self.ny.insert_one(self._convert(ny_data)).inserted_id)

    def _convert(self, census_data):
        census_data[c.SEX] = 'M' if census_data[c.SEX] == 1 else 'F'
        census_data[c.FLAG_HISPANIC] = True if census_data[c.FLAG_HISPANIC] == 2 else False

        census_data[c.RACE] = {
            1: 'White alone',
            2: 'Black or African American alone',
            3: 'American Indian and Alaska Native alone',
            4: 'Asian alone',
            5: 'Native Hawaiian and Other Pacific Islander alone',
            6: 'White and Black or African American',
            7: 'White and American Indian and Alaska Native',
            8: 'White and Asian',
            9: 'White and Native Hawaiian and Other Pacific Islander',
            10: 'Black or African American and American Indian and Alaska Native',
            11: 'Black or African American and Asian',
            12: 'Black or African American and Native Hawaiian and Other Pacific Islander',
            13: 'American Indian and Alaska Native and Asian',
            14: 'American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander',
            15: 'Asian and Native Hawaiian and Other Pacific Islander',
            16: 'White and Black or African American and American Indian and Alaska Native',
            17: 'White and Black or African American and Asian',
            18: 'White and Black or African American and Native Hawaiian and Other Pacific Islander',
            19: 'White and American Indian and Alaska Native and Asian',
            20: 'White and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander',
            21: 'White and Asian and Native Hawaiian and Other Pacific Islander',
            22: 'Black or African American and American Indian and Alaska Native and Asian',
            23: 'Black or African American and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander',
            24: 'Black or African American and Asian and Native Hawaiian and Other Pacific Islander',
            25: 'American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander',
            26: 'White and Black or African American and American Indian and Alaska Native and Asian',
            27: 'White and Black or African American and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander',
            28: 'White and Black or African American and Asian and Native Hawaiian and Other Pacific Islander',
            29: 'White and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander',
            30: 'Black or African American and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander',
            31: 'White and Black or African American and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander'
        }.get(census_data[c.RACE])
        if census_data[c.FLAG_HISPANIC]:
            census_data[c.RACE] = census_data[c.RACE] + ' Hispanic'

        census_data[c.AGE_RANGE] = {
            1: '0-4',
            2: '5-9',
            3: '10-14',
            4: '15-19',
            5: '20-24',
            6: '25-29',
            7: '30-34',
            8: '35-39',
            9: '40-44',
            10: '45-49',
            11: '50-54',
            12: '55-59',
            13: '60-64',
            14: '65-69',
            15: '70-74',
            16: '75-79',
            17: '80-84',
            18: '>85'
        }.get(census_data[c.AGE_RANGE])

        return census_data
