from pymongo import MongoClient
import pandas as pd

FILE = "Library_Usage.csv"
CONNECTION_STRING = "mongodb+srv://sol21212:246810@cluster0.zvhaejl.mongodb.net/?retryWrites=true&w=majority&tlsAllowInvalidCertificates=true"


def read_data(filename: str):
    """
    This function reads the data from a csv file and returns it as a list of dictionaries
    :param filename: The name of the .csv file
    :return: dict
    """
    df = pd.read_csv(filename)
    data_dict = df.to_dict(orient='records')
    return data_dict


def db_connection():
    """
    This function creates a connection to the database and retrurns databse Lab3
    :return: database Lab3
    """
    client = MongoClient(CONNECTION_STRING)
    db = client.Lab3
    return db


def insert_data(db, data: dict):
    """
    This function inserts the data into the database
    :param db: database instance
    :param data: list of dictionaries
    :return: None
    """
    collection = db['library']
    collection.insert_many(data)


def run():
    """
    This function runs the whole process
    :return: None
    """
    data = read_data(FILE)
    db = db_connection()
    insert_data(db, data)


if __name__ == "__main__":
    run()
