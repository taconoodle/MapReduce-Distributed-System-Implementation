import boto3
from botocore.exceptions import *
from botocore.client import Config
import json
from itertools import groupby
from contextlib import contextmanager


RUSTFS_USERNAME = 'admin'
RUSTFS_PASSWORD = 'admin'
RUSTFS_URL = 'http://localhost:9000'

class S3ConnectionError(Exception):
    pass

class S3Storage:
    
    def __init__(self):
        self.conn = None

    @contextmanager
    def _handle_errors(self):
        try:
            yield
        except EndpointConnectionError as e:
            raise S3ConnectionError('Could not reach S3 endpoint')
        except ClientError as e:
            code = e.response['Error']['Code']
            match code:
                case 'InvalidAccessKeyId':
                    raise S3ConnectionError('Invalid username')
                case 'SignatureDoesNotMatch':
                    raise S3ConnectionError('Invalid password')
                case 'BucketAlreadyExists':
                    raise S3ConnectionError('Bucket already exists')
                case 'InvalidBucketName':
                    raise S3ConnectionError('Invalid bucket name')
                case 'TooManyBuckets':
                    raise S3ConnectionError('Too many buckets')
                case 'NoSuchBucket':
                    raise S3ConnectionError('Bucket does not exist')
                case 'NoSuchKey':
                    raise S3ConnectionError('Key does not exist')
                case _:
                    raise S3ConnectionError(f'Unknown error: {e}')

    def init_s3(self):
        with self._handle_errors():
            self.conn = boto3.client(
                's3',
                endpoint_url=RUSTFS_URL,
                aws_access_key_id=RUSTFS_USERNAME,
                aws_secret_access_key=RUSTFS_PASSWORD,
                config=Config(signature_version='s3v4'), # The version of the signatures the authenticates AWS requests. RustFS wants s3v4
                region_name='us-east-1' # Apparently RustFS expects us to use us-east-1
            )
            return self.conn

    def create_bucket(self, bucket_name):
        with self._handle_errors():
            response = self.conn.create_bucket(Bucket=bucket_name)
            return response
    
    def create_object(self, bucket, key, body):
        with self._handle_errors():
            response = self.conn.put_object(
                Bucket=bucket,
                Key=key,
                Body=body
            )
            return response

    def upload_to_bucket(self, bucket_name, filename, key=None):
        with self._handle_errors():
            if key is None:
                key = filename
            self.conn.upload_file(Bucket=bucket_name, Key=key, Filename=filename)

    def download_from_bucket(self, bucket_name, key, filename=None):
        with self._handle_errors():
            if filename is None:
                filename = key
            self.conn.download_file(Bucket=bucket_name, Key=key, Filename=filename)

    def delete_from_bucket(self, bucket_name, key):
        with self._handle_errors():
            response = self.conn.delete_object(Bucket=bucket_name, Key=key)
            return response

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def copy_file_part(self, upload_id, src_bucket, src_key, dst_bucket, dst_key, byte_amount_mb=64, byte_offset=0, part_number=1):
        if byte_amount_mb <= 0:
            raise S3ConnectionError('Byte amount must be at least 0')
        if byte_offset < 0:
            raise S3ConnectionError('Byte offset must be greater than 0')
        if part_number < 1 or part_number > 10000:
            raise S3ConnectionError('Invalid part number. Part number must be between 1 and 10000')

        with self._handle_errors():
            file_size = self.conn.head_object(Bucket=src_bucket, Key=src_key)['ContentLength']
            byte_amount = byte_amount_mb * (1024 ** 2)

            first_byte = byte_offset
            last_byte = min(byte_offset + byte_amount - 1, file_size - 1)
            byte_range = f'bytes={first_byte}-{last_byte}'

            copy_source = {
                'Bucket': src_bucket,
                'Key': src_key
            }

            response = self.conn.upload_part_copy(
                UploadId=upload_id,
                Bucket=dst_bucket,
                Key=dst_key,
                PartNumber=part_number, # PartNumber needs to be 1 to 10000 (Source: RTFM)
                CopySource=copy_source,
                CopySourceRange=byte_range
            )
            return response

    def stream_file_pairs(self, bucket, key):
        response = self.conn.get_object(
            Bucket=bucket,
            Key=key
        )
        for line in response['Body'].iter_lines():
            pairs = json.loads(line)
            for key, value in pairs.items():
                yield key, value


    def get_json_body(self, bucket, key):
        body = self.conn.get_object(
            Bucket=bucket,
            Key=key
        )['Body']
        return json.load(body)

    def sort_json(self, src_bucket, src_key, dst_bucket, dst_key):
        data = self.get_json_body(src_bucket, src_key)

        data = dict(sorted(data.items(), key=lambda item: item[0]))
        self.conn.put_object (
            Bucket=dst_bucket,
            Key=dst_key,
            Body=json.dumps(data)
        )


    def create_paginator(self, bucket, prefix=''):
        paginator = self.conn.get_paginator('list_objects_v2')
        paginator_parameters = {
            'Bucket': bucket,
            'Prefix': prefix,
            'Delimiter': '/'
        }
        page_iterator = paginator.paginate(**paginator_parameters)
        return page_iterator

    def stream_paginator_pages(self, bucket, prefix=''):
        paginator = self.create_paginator(bucket, prefix)
        for page in paginator:
            yield page

    def stream_keys_in_dir(self, bucket, prefix):
        for page in self.stream_paginator_pages(bucket, prefix):
            for contents in page['Contents']:
                yield contents['Key']

    def upload_sorted_data_in_chunks(self, bucket, key, data):
        mpu = self.conn.create_multipart_upload(
            Bucket=bucket,
            Key=key
        )
        upload_id = mpu['UploadId']
        parts = []
        part_number = 1
        buffer = []
        buffer_size = 0
        for key, group in groupby(data, key=lambda pair: pair[0]):
            values = [value for _, value in group]
            line = json.dumps({key: values}) + '\n'
            line = line.encode('utf-8')
            buffer_size += len(line)

            if buffer_size >= (5 * (1024 ** 2)):
                chunk = b''.join(buffer)
                response = self.conn.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk
                )
                parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
                part_number += 1
                buffer = []
                buffer_size = 0

        # Handle last chunk
        if buffer:
            chunk = b''.join(buffer)
            response = self.conn.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=chunk
            )
            parts.append({'PartNumber': part_number, 'ETag': response['ETag']})

        self.conn.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )