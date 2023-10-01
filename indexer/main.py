import datetime
import time
from typing import List, Optional

import numpy as np
import redis
import requests
from minio import Minio, S3Error
from redis import ResponseError
from redis.commands.search.field import TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from requests import JSONDecodeError

from chunker import Chunker
from settings import *


def encode(lines: List[str]) -> Optional[List[List[float]]]:
    response = requests.post(EMBEDDINGS_ENDPOINT, json={"input": lines})
    try:
        json = response.json()
        response.close()
        return [entry["embedding"] for entry in json["data"]]
    except JSONDecodeError:
        response.close()
        return None


def batch_delete_keys(redis_client: redis.Redis, pattern: str) -> int:
    """
    s. https://gist.github.com/dingmaotu/b465509f5c5d54dceacf5a2eb985c739
    """
    item_count = 0
    batch_size = 100000
    keys = []

    for k in redis_client.scan_iter(pattern, count=batch_size):
        keys.append(k)
        if len(keys) >= batch_size:
            item_count += len(keys)
            redis_client.delete(*keys)
            keys = []

    if len(keys) > 0:
        item_count += len(keys)
        redis_client.delete(*keys)

    return item_count


def main():
    print("Job started")

    minio = Minio(
        S3_API_HOST + ":" + S3_API_PORT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=False
    )

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True
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

        # collect objects in S3
        dirs = [(S3_BUCKET, "")]
        files = []
        while len(dirs) > 0:
            bucket, prefix = dirs.pop()
            file_objects = minio.list_objects(bucket, prefix=prefix)
            for obj in file_objects:
                if obj.is_dir:
                    dirs.append((bucket, obj.object_name))
                else:
                    if obj.object_name.lower().endswith(".txt"):
                        files.append(obj)

        # build indices
        for obj in files:
            print("\n" + str(vars(obj)))

            path = f"{obj.bucket_name}/{obj.object_name}"

            # 1. check ETag of object, if object not modified and index exists, skip it
            metadata = redis_client.hgetall("metadata:" + path)
            if (metadata is not None and isinstance(metadata, dict) and
                    "object_etag" in metadata.keys() and metadata["object_etag"] == obj.etag and
                    "index_dt" in metadata.keys()):
                print(f"{path} unchanged, indexed on {metadata['index_dt']}")
                continue

            # 2. split, vectorize and save to Redis
            try:
                response = minio.get_object(obj.bucket_name, obj.object_name)

                index_name = "idx:" + path

                # delete stale index
                try:
                    redis_client.ft(index_name).info()
                    redis_client.ft(index_name).dropindex()
                    print("Dropping index (index will be recreated): " + index_name)
                except ResponseError:
                    print("Index not found: " + index_name)

                # delete stale data
                item_count = batch_delete_keys(redis_client, f"{path}:*")
                print(f"Removed {item_count} stale entries")

                data = response.read()
                data_decoded = data.decode("UTF-8")

                # lazily split to chunks containing 32 words (with overlap)
                chunker = Chunker(text=data_decoded, chunk_size=32)
                for i, chunk in enumerate(chunker.get_chunks(), start=1):
                    text = " ".join(chunk)
                    embeddings = encode([text])
                    if embeddings is not None:
                        name = path + ":" + str(i)
                        embedding = np.array(embeddings[0]).astype(np.float32).tobytes()
                        redis_client.hset(name, mapping={"text": text, "embedding": embedding})
                        i += 1
                    time.sleep(50 / 1000)  # throttle
            finally:
                response.close()
                response.release_conn()

            # create index
            schema = (
                TextField("text", no_stem=True, as_name="text"),
                VectorField(
                    "embedding",
                    "HNSW",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": vector_dimension,
                        "DISTANCE_METRIC": "COSINE",
                        "M": 20,  # HNSW parameter
                        "EF_CONSTRUCTION": 100  # HNSW parameter
                    },
                    as_name="vector"
                )
            )
            definition = IndexDefinition(prefix=[path + ":"], index_type=IndexType.HASH)
            redis_client.ft(index_name).create_index(fields=schema, definition=definition)
            print("Index created: " + index_name)
            res = redis_client.ft(index_name).info()
            print(res)

            print(f"Saving metadata for {path}")
            redis_client.hset("metadata:" + path, mapping={
                "object_name": path,
                "object_etag": obj.etag,
                "index_name": index_name,
                "index_dt": str(datetime.datetime.now())
            })
    except S3Error as e:
        print("error occurred.", e)
    finally:
        redis_client.close()


if __name__ == '__main__':
    main()
