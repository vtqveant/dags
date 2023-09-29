import os
import time
from typing import List, Optional

import redis
import requests
from minio import Minio, S3Error
from redis.commands.search.field import TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from requests import JSONDecodeError

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
EMBEDDINGS_ENDPOINT = os.getenv("EMBEDDINGS_ENDPOINT")
S3_API_HOST = os.getenv("S3_API_HOST")
S3_API_PORT = os.getenv("S3_API_PORT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")


def encode(lines: List[str]) -> Optional[List[List[float]]]:
    response = requests.post(EMBEDDINGS_ENDPOINT, json={"input": lines})
    try:
        json = response.json()
        response.close()
        return [entry["embedding"] for entry in json["data"]]
    except JSONDecodeError:
        response.close()
        return None


def main():
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True
    )

    minio = Minio(
        S3_API_HOST + ":" + S3_API_PORT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=False
    )

    try:
        found = minio.bucket_exists(S3_BUCKET)
        if not found:
            print(f"Bucket `{S3_BUCKET}` not found")
            return

        # test embeddings endpoint and get vector dimension
        embeddings = encode([""])
        vector_dimension = len(embeddings[0])

        # List objects information whose names starts with "my/prefix/".
        print(f"\nObjects in bucket `{S3_BUCKET}` (prefix '11/'):")
        objects = minio.list_objects(S3_BUCKET, prefix="11/")
        for obj in objects:
            print(vars(obj))

            if not obj.is_dir:
                try:
                    response = minio.get_object(obj.bucket_name, obj.object_name)

                    key_prefix = f"{obj.bucket_name}/{obj.object_name}:"
                    index_name = f"idx:{obj.bucket_name}_{obj.object_name}_vss"

                    print("\nDropping index " + index_name)
                    res = redis_client.ft(index_name).dropindex()
                    print(res)

                    i = 1
                    for line in response.readlines():
                        text = line.decode("UTF-8")
                        embeddings = encode(text)
                        redis_client.json().set(key_prefix + str(i), "$", {"text": text, "embedding": embeddings[0]})
                        time.sleep(10)  # since embeddings endpoint does not have throttling yet
                        i += 1
                finally:
                    response.close()
                    response.release_conn()

                # create index
                schema = (
                    TextField("$.text", no_stem=True, as_name="text"),
                    VectorField(
                        "$.embedding",
                        "FLAT",
                        {
                            "TYPE": "FLOAT32",
                            "DIM": vector_dimension,
                            "DISTANCE_METRIC": "COSINE",
                        },
                        as_name="vector",
                    ),
                )
                definition = IndexDefinition(
                    prefix=[key_prefix],
                    index_type=IndexType.JSON
                )

                res = redis_client.ft(index_name).create_index(fields=schema, definition=definition)
                print("\nIndex created")
                print(res)

    except S3Error as e:
        print("error occurred.", e)


if __name__ == '__main__':
    main()
