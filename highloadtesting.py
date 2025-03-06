import tarfile
import io
import json
import time
import weaviate
from weaviate.classes.init import Auth
from wasabi import msg


dotenv.load_dotenv()

openai_api_key = os.getenv("OPENAI_API_KEY")
WEAVIATE_URL = os.getenv("WCD_URL")
WEAVIATE_API_KEY = os.getenv("WCD_API_KEY")



# Constants
SPHERE_DATASET = "sphere.100M.jsonl.tar.gz"
MAX_IMPORT = 1_000_000
WEAVIATE_URL = "https://mbh8plpcqvygqxbeca9xuw.c0.europe-west3.gcp.weaviate.cloud"
WEAVIATE_API_KEY = "Jr6kc4bJTD08OvRA9lBKrUOB98SCdocSF7si"
COLLECTION_NAME = "pages"


def parse_json(line: str):
    """Parse a JSON line safely."""
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        msg.fail("Error parsing JSONL")
        return None


def connect_to_weaviate():
    """Establish a connection to the Weaviate cloud."""
    return weaviate.connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL,
        auth_credentials=Auth.api_key(WEAVIATE_API_KEY),
    )


def recreate_collection(client):
    """Delete the existing collection and create a new one."""
    if client.collections.exists(COLLECTION_NAME):
        client.collections.delete(COLLECTION_NAME)
    return client.collections.create(COLLECTION_NAME)


def add_object_to_batch(batch, parsed_line):
    """Adds an object to the batch and handles error checking."""
    batch.add_object(
        properties={
            'title': parsed_line['title'],
            'url': parsed_line['url'],
            'raw': parsed_line['raw']
        },
        vector=parsed_line['vector'],
        uuid=parsed_line['id']
    )
    
    if batch.number_errors > 10:
        msg.fail("Batch import stopped due to excessive errors.")
        exit(1)
    
    return batch


def process_tar_file(collection):
    """Process and import data from the tar.gz file."""
    counter = 0
    
    with tarfile.open(SPHERE_DATASET, "r|gz") as tar:
        for member in tar:
            if member.isfile():
                f = tar.extractfile(member)
                if f:
                    with collection.batch.dynamic() as batch:
                        for line in io.BufferedReader(f):
                            
                            parsed_line = parse_json(line.decode("utf-8").strip())
                            
                            if not parsed_line:
                                continue
                            
                            if not add_object_to_batch(batch, parsed_line):
                                return
                            
                            counter += 1
                            if counter >= MAX_IMPORT:
                                return
                            
                            if counter % 1_000 == 0:
                                msg.info(f"Processing batch {counter // 1_000}: count = {counter}")


def main():
    """Main function to orchestrate data loading into Weaviate."""

    start_time = time.time()

    client = connect_to_weaviate()
    msg.good("Connected to Weaviate client")
    
    collection = recreate_collection(client)
    msg.good("(Re)created collection")
    
    process_tar_file(collection)
    msg.good("Data import complete")
    
    client.close()
    msg.good("Closed Weaviate client connection")
    msg.good(f"Execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()