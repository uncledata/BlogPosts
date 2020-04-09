from pymongo import MongoClient
from faker import Faker
from faker.providers import job, company, phone_number, date_time
from typing import Dict, List
from multiprocessing import Pool
from collections import deque

def mongo_connect(mongo_uri: str, db_name: str, collection_name: str) -> MongoClient:
    return MongoClient(mongo_uri)[db_name][collection_name]


def create_mock_person() -> Dict:
    fake = Faker()
    fake.add_provider([job, company, phone_number, date_time])
    return {
        "name": fake.name(),
        "job": fake.job(),
        "company": fake.company(),
        "phone_number": fake.phone_number(),
        "date_created": fake.date(),
    }


def create_list_of_people(count: int) -> None:
    mongo = mongo_connect(
        mongo_uri="mongodb://localhost:27017",
        db_name="mock_db",
        collection_name="mock_collection",
    )
    people_list = deque()
    for _ in range(0, count):
        people_list.append(create_mock_person())
    mongo.insert_many(people_list)


def main():
    for i in range(0, 6000):
        print(i)
        num_of_workers = 10
        people_per_worker = [100] * num_of_workers
        people = []
        with Pool(num_of_workers) as executor:
            executor.map(create_list_of_people, people_per_worker)


if __name__ == "__main__":
    main()
