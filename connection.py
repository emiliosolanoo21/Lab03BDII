from pymongo import MongoClient
import pandas as pd

FILE = "Library_Usage.csv"
CONNECTION_STRING = "mongodb+srv://sol21212:246810@cluster0.zvhaejl.mongodb.net/?retryWrites=true&w=majority"


def read_data(filename: str):
    df = pd.read_csv(filename)
    data_dict = df.to_dict(orient='records')
    return data_dict


def db_connection():
    client = MongoClient(CONNECTION_STRING)
    db = client.Lab3
    return db


def insert_data(db, data: dict):
    collection = db['library']
    collection.insert_many(data)


def run():
    data = read_data(FILE)
    db = db_connection()
    insert_data(db, data)


if __name__ == "__main__":
    run()
