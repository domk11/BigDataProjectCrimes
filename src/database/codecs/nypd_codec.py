from bson import ObjectId

from src.model import Nypd
from ..contracts import nypd_contract as c


_NYPD_DOCUMENT_BINDINGS = {
    'id': c.ID,
    'date': c.DATE,
    'time': c.TIME,
    'precinct': c.PRECINCT,
    'offense_code': c.OFFENSE_CODE,
    'offense_description': c.OFFENSE_DESCRIPTION,
    'crime_outcome': c.CRIME_OUTCOME,
    'level_offense': c.LEVEL_OFFENSE,
    'borough': c.BOROUGH,
    'latitude': c.LATITUDE,
    'longitude': c.LONGITUDE,
    'age': c.AGE,
    'race': c.RACE,
    'sex': c.SEX
}


_UNSAFE_NYPD_FIELDS = [c.ID]


def nypd_from_document(doc: dict) -> Nypd:
    if doc is None:
        doc = {}

    def get_prop(k):
        if k == c.ID:
            return str(doc[c.ID]) \
                if c.ID in doc else None
        return doc.get(k)

    return Nypd(**{model_property: get_prop(doc_field)
                     for model_property, doc_field in _NYPD_DOCUMENT_BINDINGS.items()})


def nypd_to_document(nypd: Nypd, strip_unsafe=True) -> dict:
    doc = {doc_field: getattr(nypd, model_property)
           for model_property, doc_field in _NYPD_DOCUMENT_BINDINGS.items()
           if doc_field not in _UNSAFE_NYPD_FIELDS}

    if not strip_unsafe:
        doc[c.ID] = ObjectId(nypd.id)

    return doc