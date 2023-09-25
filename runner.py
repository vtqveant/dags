#!/usr/bin/env python3

import os
from typing import List, Optional

import requests
from minio import Minio
from minio.error import S3Error
from requests import JSONDecodeError

S3_API_HOST = os.getenv("S3_API_HOST", default="s3.eventflow.ru")
S3_API_PORT = os.getenv("S3_API_PORT", default="30900")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", default="Vw4FUH4zzKY62ZYUcSS1")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", default="EUf4skSLyoAfa9qiBnniGIG0HQXms153biNFHGDv")
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
    if S3_ACCESS_KEY is None or S3_SECRET_KEY is None:
        print("No credentials provided")
        return

    client = Minio(
        S3_API_HOST + ":" + S3_API_PORT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=False
    )

    found = client.bucket_exists("literature")
    if not found:
        print("Bucket 'literature' not found")
        return

    # List objects information.
    objects = client.list_objects("literature")
    print("Objects in bucket 'literature':")
    for obj in objects:
        print(vars(obj))

    # List objects information whose names starts with "my/prefix/".
    print("\nObjects in bucket 'literature' (prefix '11/'):")
    objects = client.list_objects("literature", prefix="11/")
    for obj in objects:
        print('\n')
        print(vars(obj))

        # Get data of an object.
        if not obj.is_dir:
            try:
                response = client.get_object(obj.bucket_name, obj.object_name)

                i = 0
                lines = []
                for line in response.readlines():
                    s = line.decode("UTF-8")
                    lines.append(s)
                    print(s)
                    i += 1
                    if i >= 5:
                        break

                embeddings = encode(lines)
                for embedding in embeddings:
                    print(embedding)
            finally:
                response.close()
                response.release_conn()


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)