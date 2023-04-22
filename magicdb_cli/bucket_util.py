import sys

import boto3
from botocore.config import Config
from os import path


def bucket_list_file_on_cur_dir(bucket: str, prefix: str, **kwargs):
    config = None
    if bucket.startswith("oss://"):
        bucket = bucket[6:]  # strip "oss://"
        config = Config(s3={"addressing_style": "virtual", "signature_version": 's3v4'})

    elif bucket.startswith("s3://"):
        bucket = bucket[5:]
    s3_resource = boto3.resource('s3', config=config,
                                 **kwargs)
    bucket = s3_resource.Bucket(bucket)
    list = bucket.objects.filter(Prefix=prefix)
    files = []
    for o in list:
        if o.key.endswith("/"):
            continue
        dirname = path.dirname(o.key)
        prefix = path.dirname(prefix)
        if dirname == prefix:
            files.append(o.key)
    return files


def bucket_download_file(bucket: str, remote_path: str, dst_path: str, **kwargs):
    config = None
    if bucket.startswith("oss://"):
        bucket = bucket[6:]  # strip "oss://"
        config = Config(s3={"addressing_style": "virtual", "signature_version": 's3v4'})
    elif bucket.startswith("s3://"):
        bucket = bucket[5:]
    s3c = boto3.client('s3', config=config, **kwargs)

    with open(dst_path, 'wb') as f:
        s3c.download_fileobj(bucket, remote_path, f)


def bucket_upload_file(local_path: str, bucket: str, remote_path: str, **kwargs):
    print("upload file ", local_path, " to ", path.join(bucket, remote_path))
    config = None
    if bucket.startswith("oss://"):
        bucket = bucket[6:]  # strip "oss://"
        config = Config(s3={"addressing_style": "virtual", "signature_version": 's3v4'})
    elif bucket.startswith("s3://"):
        bucket = bucket[5:]
    s3_resource = boto3.resource('s3', config=config, **kwargs)
    s3_resource.meta.client.upload_file(local_path, bucket, remote_path)

    pass


if __name__ == "__main__":
    endpoint = 'https://oss-cn-shanghai.aliyuncs.com'
    access_key_id = 'LTAI5t8BpVYnfpFmM8aNsoAN'
    secret_access_key = sys.argv[1]
    kargs = {
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key,
        "endpoint_url": endpoint,
        "region_name": "cn-shanghai"
    }
    x = bucket_list_file_on_cur_dir(bucket="oss://uopensail-test", prefix="test/parquet/", **kargs)
    for i in x:
        print(i)

    bucket_upload_file("/tmp/dnscache.json", "oss://uopensail-test", "test/x/dnscache.json", **kargs)
