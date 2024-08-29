import os, dotenv
import json
import time
import weaviate
import weaviate.classes.config as wvc
import weaviate.classes as wc
from weaviate.classes.config import Configure, Property, DataType, VectorDistances
from weaviate.classes.tenants import Tenant
import numpy as np
from weaviate.classes.init import AdditionalConfig, Timeout, Auth
from datetime import datetime, timedelta
from loguru import logger

#  This test scripts creates 1000 tenants in a call and loads 10K objects to each of these tenants

dotenv.load_dotenv()

openai_api_key = os.getenv("OPENAI_API_KEY")
WCD_URL = os.getenv("WCD_URL")
WCD_API_KEY = os.getenv("WCD_API_KEY")

# Variables
#  Download the sphere dataset from https://weaviate.io/blog/sphere-dataset-in-weaviate#importing-sphere-with-python

SPHERE_DATASET = "sphere.100k.jsonl"  # update to match your filename


def get_client(URL, APIKEY):
    # client = weaviate.connect_to_local()
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=URL,
        auth_credentials=weaviate.auth.AuthApiKey(APIKEY),
        additional_config=wc.init.AdditionalConfig(
            timeout=wc.init.Timeout(init=10)
        ),  # Values in seconds
        #
    )
    return client


def read_jsonl_file(num_objects):
    objects = []
    with open(SPHERE_DATASET, 'r') as jsonl_file:
        for i , line in enumerate(jsonl_file, start=10000):
            if i >= 10000 + num_objects:
                break
            json_object = json.loads(line.strip())
            objects.append(json_object)
    return objects


# Import the data, Weaviate will use the auto-schema function to
# create the other properties and other default settings.
def ingest_data(collection, num_objects, name_of_tenant):
    counter = 0
    start = time.time()
    data_objects = read_jsonl_file(num_objects)
    print(data_objects)
    cl_collection = collection.with_consistency_level(wvc.ConsistencyLevel.QUORUM)
    with cl_collection.batch.dynamic() as batch:
        for obj in data_objects:
            properties = {
                "url": obj["url"],
                "title": obj["title"],
                "raw": obj["raw"],
                "sha": obj["sha"],
            }
            batch.add_object(
                properties=properties,
                vector=obj["vector"],
                uuid=obj["id"],
            )
        counter += 1
        if collection.batch.failed_objects:
            for failed_object in collection.batch.failed_objects:
                print(
                    f"Failed to add object with UUID {failed_object.uuid} in tenant {name_of_tenant}: {failed_object.message}"
                )
        print(
            f"Imported {num_objects} objects in {time.time() - start} in tenant {name_of_tenant}"
        )


def add_objects_to_tenants():
    collection = client.collections.get("Testingdynamicindexings")
    tenants = [key for key in collection.tenants.get().keys()]
    for tenant in tenants:
        ingest_data(collection.with_tenant(tenant), 5, tenant)


if __name__ == "__main__":
    client = get_client(WCD_URL, WCD_API_KEY)
    logger.info("Adding objects to tenants")
    add_objects_to_tenants()
