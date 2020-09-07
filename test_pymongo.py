from src.database import connect_db, Database


db = Database(connect_db().get_database())

coll = db.get_collection()
for i in coll:
    print(i)


b1 = db.get_race('WHITE', 10000)
print(b1)

b2 = db.get_borough('bronx', 100)
print([res.to_dict() for res in b2])
