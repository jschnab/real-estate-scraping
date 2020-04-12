import os

import boto3


def list_objects(bucket, prefix):
    """
    Get the list of S3 objects under a specific prefix, with pagination
    management.

    :param str bucket: name of the S3 bucket
    :param str key_prefix: prefix of the S3 objects keys
    :return list[str]: list of S3 object keys
    """
    client = boto3.client("s3")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    token = ""
    keys = []
    while token is not None:
        if token != "":
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        contents = response.get("Contents")
        for obj in contents:
            key = obj.get("Key")
            if key[-1] != "/":
                keys.append(key)
    return keys


def download_files(bucket, key_prefix, destination):
    """
    Download all the files from an S3 bucket under a specific prefix.

    :param str bucket: name of the S3 bucket
    :param str key_prefix: prefix of the S3 objects keys
    :param str destination: directory where to store downloaded files
    """
    client = boto3.client("s3")
    objects = list_objects(bucket, key_prefix)
    for obj in objects:
        file_name = os.path.join(destination, os.path.split(obj)[-1])
        client.download_file(bucket, obj, file_name)
