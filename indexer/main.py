import datetime
import os
import time
from typing import List, Optional

import redis
import requests
from minio import Minio, S3Error
from redis import ResponseError
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
    print("Job started")

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
        print("Testing embeddings endpoint: " + EMBEDDINGS_ENDPOINT)
        embeddings = encode([""])
        if embeddings is None:
            print("Not OK")
            return
        vector_dimension = len(embeddings[0])
        print("OK")
        print("Embeddings dimension: " + str(vector_dimension))

        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, username=REDIS_USERNAME, password=REDIS_PASSWORD,
                                   decode_responses=True)

        # List objects information whose names start with "my/prefix/".
        print(f"\nObjects in bucket `{S3_BUCKET}` (prefix '11/'):")
        objects = minio.list_objects(S3_BUCKET, prefix="11/")
        for obj in objects:
            print(vars(obj))

            path = f"{obj.bucket_name}/{obj.object_name}"

            if obj.is_dir:
                continue

            # 1. check ETag of object, if object not modified and index exists, skip it
            metadata = redis_client.hgetall("metadata:" + path)
            if (metadata is not None and isinstance(metadata, dict) and
                    "ETag" in metadata.keys() and metadata["ETag"] == obj.etag and
                    "index_ts" in metadata.keys()):
                print(f"{path} unchanged, indexed on {metadata['index_ts']}")
                continue

            # 2. reindex
            try:
                response = minio.get_object(obj.bucket_name, obj.object_name)

                index_name = "idx:" + path
                try:
                    redis_client.ft(index_name).info()
                    redis_client.ft(index_name).dropindex()
                    print("Dropping index (index will be recreated): " + index_name)
                except ResponseError:
                    print("Index not found: " + index_name)

                i = 1
                for line in response.readlines():
                    text = line.decode("UTF-8")
                    embeddings = encode(text)
                    if embeddings is not None:
                        redis_client.json().set(path + ":" + str(i), "$", {"text": text, "embedding": embeddings[0]})
                        i += 1
                    time.sleep(50 / 1000)  # throttle
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
            definition = IndexDefinition(prefix=[path + ":"], index_type=IndexType.JSON)
            res = redis_client.ft(index_name).create_index(fields=schema, definition=definition)
            print("Index created: " + index_name)
            print(res)

            print(f"Saving metadata for {path}")
            redis_client.hset("metadata:" + path, "ETag", obj.etag)
            redis_client.hset("metadata:" + path, "index_ts", str(datetime.datetime.now()))
    except S3Error as e:
        print("error occurred.", e)
    finally:
        redis_client.close()


if __name__ == '__main__':
    main()
