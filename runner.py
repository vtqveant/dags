#!/usr/bin/env python3

import os
from minio import Minio
from minio.error import S3Error


S3_API_HOST = os.getenv("S3_API_HOST", default="s3.eventflow.ru")
S3_API_PORT = os.getenv("S3_API_PORT", default="30900")


def main():
    client = Minio(
        S3_API_HOST + ":" + S3_API_PORT,
        access_key="Vw4FUH4zzKY62ZYUcSS1",
        secret_key="EUf4skSLyoAfa9qiBnniGIG0HQXms153biNFHGDv",
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
        print(obj)

    # List objects information whose names starts with "my/prefix/".
    print("\nObjects in bucket 'literature' (prefix '11/'):")
    objects = client.list_objects("literature", prefix="11/")
    for obj in objects:
        print(obj)


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)