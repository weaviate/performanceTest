# Performance Testing scripts

This repo contains scripts to do load and performance testing


## Pre-Condition

1. To install the dependency, run the following command

`pip install -r requirements.txt`

2. Copy the env-example file to .env and fill the necessary URL's and API keys to run the performance test

```
OPENAI_API_KEY=
WCD_URL=
WCD_API_KEY=

```

## Steps to performance tests
1. Confirm you have set your env variables
2. Run `dynamicindexing_load.py` to run the load testing of dynamic vector indexing collection
