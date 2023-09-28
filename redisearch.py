import os
from typing import List, Optional

import redis
import requests
from redis.commands.search.field import (
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from requests import JSONDecodeError

REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
EMBEDDINGS_ENDPOINT = os.getenv("EMBEDDINGS_ENDPOINT", default="https://api.eventflow.ru/v1/embeddings/")


def encode(lines: List[str]) -> Optional[List[float]]:
    response = requests.post(EMBEDDINGS_ENDPOINT, json={"input": lines})
    try:
        json = response.json()
        response.close()
        return [entry["embedding"] for entry in json["data"]]
    except JSONDecodeError:
        response.close()
        return None


def main():
    url = "https://raw.githubusercontent.com/bsbodden/redis_vss_getting_started/main/data/bikes.json"
    response = requests.get(url)
    bikes = response.json()

    client = redis.Redis(
        host='localhost',
        port=6379,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True
    )

    # store data to Redis
    pipeline = client.pipeline()
    for i, bike in enumerate(bikes, start=1):
        redis_key = f"bikes:{i:03}"
        pipeline.json().set(redis_key, "$", bike)
    res = pipeline.execute()

    # res = client.json().get("bikes:010", "$.model")
    # print(res)

    # add embeddings
    keys = sorted(client.keys("bikes:*"))
    print(keys)

    descriptions = client.json().mget(keys, "$.description")
    descriptions = [item for sublist in descriptions for item in sublist]
    embeddings = [encode(description) for description in descriptions]

    VECTOR_DIMENSION = len(embeddings[0])

    pipeline = client.pipeline()
    for key, embedding in zip(keys, embeddings):
        pipeline.json().set(key, "$.description_embeddings", embedding)
    pipeline.execute()

    # create index
    schema = (
        TextField("$.model", no_stem=True, as_name="model"),
        TextField("$.brand", no_stem=True, as_name="brand"),
        NumericField("$.price", as_name="price"),
        TagField("$.type", as_name="type"),
        TextField("$.description", as_name="description"),
        VectorField(
            "$.description_embeddings",
            "FLAT",
            {
                "TYPE": "FLOAT32",
                "DIM": VECTOR_DIMENSION,
                "DISTANCE_METRIC": "COSINE",
            },
            as_name="vector",
        ),
    )
    definition = IndexDefinition(prefix=["bikes:"], index_type=IndexType.JSON)
    res = client.ft("idx:bikes_vss").create_index(
        fields=schema, definition=definition
    )


if __name__ == '__main__':
    main()
