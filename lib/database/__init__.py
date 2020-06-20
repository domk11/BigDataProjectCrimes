from .database import Database

def connect_db():
    import pymongo
    return pymongo.MongoClient('mongodb://localhost:27017/data_science')