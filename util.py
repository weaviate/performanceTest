import weaviate
import os, dotenv
from weaviate.classes.init import Auth
from weaviate.classes.init import AdditionalConfig, Timeout


dotenv.load_dotenv()

# Load enviornment variable
openai_api_key = os.getenv("OPENAI_API_KEY")
URL = os.getenv("WCD_URL")
APIKEY = os.getenv("WCD_API_KEY")


def get_wcs_client(URL, APIKEY):
    if not openai_api_key:
        raise ValueError("The OPENAI_APIKEY environment variable is not set.")
    if not URL or not APIKEY:
        raise ValueError(
            "The WCD_URL or WCD_API_KEY environment variables are not set."
        )

    client = weaviate.connect_to_wcs(
        cluster_url=URL,
        auth_credentials=Auth.api_key(APIKEY),
        skip_init_checks=True,
        additional_config=AdditionalConfig(
            timeout=Timeout(insert=120, query=120, init=120)
        ),
        # headers={"X-OpenAI-Api-Key": openai_api_key}
    )
    print(client.get_meta())

    client.connect()  # When directly instantiating, you need to connect manually
    return client
