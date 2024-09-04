import json
import os, dotenv
import time
import weaviate
import weaviate.classes.config as wvc
from weaviate.collections.collection import Collection
from typing import List
import weaviate.classes as wc

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
    )
    return client

LIMIT = 10

def query_latency(
    collection: Collection, vectors: List[List[float]]
) -> None:
    took = 0
    tenants = [key for key in collection.tenants.get().keys()]
    total_tenant = len(tenants)
    total_time = 0
    for tenant in tenants:
        start = time.time()
        objs = collection.with_tenant(tenant).query.near_vector(
            near_vector=vectors, limit=LIMIT,return_properties=[]
        )    # print(objs)
        end = time.time()
        total_time += end - start
        # print(f" Received {len(objs.objects)} objects when querying 9999 records  for tenant '{tenant}' in {end-start}s")
    average = (total_time / total_tenant)
    print(f"average time for all tenants: {average} s, total tenants: {total_tenant}, total_time: {total_time} s , qps : {1/average}")
    


def read_jsonl_file(num_objects):
    objects = []
    with open(SPHERE_DATASET) as jsonl_file:
        for i, line in enumerate(jsonl_file):
            if i >= num_objects:
                break
            json_object = json.loads(line.strip())
            objects.append(json_object)
    return objects


def run() -> None:
    client = weaviate.connect_to_local()
    collection = client.collections.get("Testingdynamicindexings")
    data_objects = read_jsonl_file(100)
    index = 0
    for data in data_objects:
        vector_test = data["vector"]
        # for ef in EF_VALUES:
        print(f"calling query for vector {index}")
        index = index + 1
        query_latency(collection, vector_test)

run()