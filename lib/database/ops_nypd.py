from bson import ObjectId

from lib.model import Nypd
from .contracts import nypd_contract as c
from .codecs import nypd_from_document, nypd_to_document


class NypdOpsMixin:

    def __init__(self, db_connection):
        super().__init__(db_connection)
        self.nypd = db_connection[c.COLLECTION_NAME]
        
    def get_nypd(self, nypd_id):
        nypd_doc = self.nypd.find_one({c.ID: ObjectId(nypd_id)})
        return nypd_from_document(nypd_doc) if nypd_doc is not None else None

    def get_borough(self, nypd_borough, limit=10):
        nypd_doc = self.nypd.find(
            {c.BOROUGH: {'$regex': f'{nypd_borough}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]

    def get_race(self, race, limit=10):
        nypd_doc = self.nypd.find(
            {c.RACE: {'$regex': f'{race}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]

    def get_sex(self, sex, limit=10):
        nypd_doc = self.nypd.find(
            {c.SEX: {'$regex': f'{sex}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]

    def get_age(self, age, limit=10):
        nypd_doc = self.nypd.find(
            {c.AGE: {'$regex': f'{age}', '$options': '-i'}}
        ).limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]

    def get_collection(self, limit=10):
        nypd_doc = self.nypd.find().limit(limit)
        return [nypd_from_document(nypd) for nypd in nypd_doc]