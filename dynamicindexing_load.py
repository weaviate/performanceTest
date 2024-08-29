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
    client = weaviate.connect_to_local()
    # client = weaviate.connect_to_weaviate_cloud(
    #     cluster_url=URL,
    #     auth_credentials=weaviate.auth.AuthApiKey(APIKEY),
    #     additional_config=wc.init.AdditionalConfig(
    #         timeout=wc.init.Timeout(init=10)
    #     ),  # Values in seconds
    #     #
    # )
    return client


    

def createCollection(client):
    try:
        if client.collections.exists("Testingdynamicindexings"):
            # delete collection "Article" - THIS WILL DELETE THE COLLECTION AND ALL ITS DATA
            client.collections.delete(
                "Testingdynamicindexings"
            )  # Replace with your collection name

        collection = client.collections.create(
            name="Testingdynamicindexings",
            properties=[
                wvc.Property(name="url", data_type=wvc.DataType.TEXT),
                wvc.Property(name="title", data_type=wvc.DataType.TEXT),
                wvc.Property(name="raw", data_type=wvc.DataType.TEXT),
                wvc.Property(name="sha", data_type=wvc.DataType.TEXT),
            ],
            vector_index_config=wvc.Configure.VectorIndex.dynamic(
                distance_metric=VectorDistances.COSINE,
                threshold=10000,
                hnsw=Configure.VectorIndex.hnsw(
                    cleanup_interval_seconds=123,
                    flat_search_cutoff=1234,
                    vector_cache_max_objects=789,
                    quantizer=wvc.Configure.VectorIndex.Quantizer.pq(),
                ),
                flat=Configure.VectorIndex.flat(
                    vector_cache_max_objects=7643,
                    quantizer=wvc.Configure.VectorIndex.Quantizer.bq(),
                ),
            ),
            multi_tenancy_config=wvc.Configure.multi_tenancy(
                True, auto_tenant_activation=True, auto_tenant_creation=True
            ),
            replication_config=wvc.Configure.replication(factor=3),
        )
        return collection
    except Exception as e:
        logger.error(f"Error creating collection: {e}")
        return False


# create class with exisiting replication factor


def create_tenants(collection):
    try:
        existing_tenants = collection.tenants.get()
        if existing_tenants:
            client.close()
            print(f"Tenants already exist in class '{collection.name}'")
            return
        else:
            collection.tenants.create(
                [Tenant(name=f"tenant_{i}") for i in range(1, 1001)]
            )
    except Exception as e:
        print(f"failed to create tenant")
        client.close()


def read_jsonl_file(num_objects):
    objects = []
    with open(SPHERE_DATASET) as jsonl_file:
        for i, line in enumerate(jsonl_file):
            if i >= num_objects:
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
    # print(data_objects)
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
        ingest_data(collection.with_tenant(tenant), 1, tenant)


if __name__ == "__main__":
    client = get_client(WCD_URL, WCD_API_KEY)
    logger.info("Creating collection")
    collection = createCollection(client)
    logger.info("Creating 1000 tenants")
    create_tenants(collection)
    logger.info("Adding objects to tenants")
    add_objects_to_tenants()
