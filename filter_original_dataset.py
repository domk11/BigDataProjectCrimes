from src.database import connect_db, Database


if __name__ == '__main__':
    db = Database(connect_db().get_database())

    filtered = db.filter_data()
    for data in filtered:
        db.put_filtered_data(data)

    filtered = db.filter_census_data()
    for data in filtered:
        db.put_filtered_census_data(data)
