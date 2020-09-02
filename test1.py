from pprint import pprint
from lib.database import connect_db, Database


db = Database(connect_db().get_database())

a = db.get_nypd('5f44f74a7213f3d09e00641c').to_dict()
print(a)

b1 = db.get_race('WHITE', 10000)
print(b1)

b2 = db.get_borough('bronx', 100)
print([res.to_dict() for res in b2])
