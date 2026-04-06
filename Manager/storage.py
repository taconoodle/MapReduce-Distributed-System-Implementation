import boto3
import botocore
from botocore.client import Config
import json
from itertools import groupby


RUSTFS_USERNAME = 'admin'
RUSTFS_PASSWORD = 'admin'
RUSTFS_URL = 'http://localhost:9000'


class S3Storage:
    
    def __init__(self):
        self.conn = None
        
    
    def init_s3(self):
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
        try:
            self.conn.create_bucket(Bucket=bucket_name)
            print(f'Bucket {bucket_name} created.')
        except self.conn.exceptions.BucketAlreadyExists:
            print(f'Bucket {bucket_name} already exists.')
        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error occured: {e}')
        return
    
    def create_object(self, bucket, key, body):
        self.conn.put_object(
            Bucket=bucket,
            Key=key,
            Body=body
        )

    def upload_to_bucket(self, bucket_name, filename, key=None):
        if key is None:
            key = filename
        self.conn.upload_file(Filename=filename, Bucket=bucket_name, Key=key)


    def download_from_bucket(self, bucket_name, key, filename=None):
        if filename is None:
            filename = key
        self.conn.download_file(Key=key, Bucket=bucket_name, Filename=filename)


    def delete_from_bucket(self, bucket_name, key):
        self.conn.delete_object(Bucket=bucket_name, Key=key)


    def close(self):
        if self.conn is not None:
            self.conn.close()
            
            
    def test_s3(self):
        bucket_name = 'test-bucket'

        self.create_bucket(bucket_name)
        print(f'Created bucket {bucket_name}')

        buckets = self.conn.list_buckets()
        print('Buckets found:')
        for bucket in buckets['Buckets']:
            print(f'\t{bucket['Name']}')

        self.conn.upload_file(Filename='test.txt', Bucket=bucket_name, Key='test.txt')
        print('File "test.txt" uploaded')

        print(f'Now printing objects in {bucket_name}:')
        objects_in_bucket = self.conn.list_objects_v2(Bucket=bucket_name)
        for obj in objects_in_bucket.get('Contents', []):
            print(f'\t{obj['Key']} - ({obj['Size']} bytes)')

        self.conn.download_file(Bucket=bucket_name, Key='test.txt', Filename='download-test.txt')
        print('File should be have been downloaded as "download-test.txt"')

        self.conn.delete_object(Bucket=bucket_name, Key='test.txt')
        print('Object deleted.')

        self.conn.delete_bucket(Bucket=bucket_name)
        print('Bucket deleted.')


    def copy_file_part(self, upload_id, src_bucket, src_key, dst_bucket, dst_key=None, byte_amount_mb=64, byte_offset=0, part_number=1):
        if dst_key is None:
            dst_key = src_key

        file_size = self.conn.head_objet(Bucket=src_bucket, Key=src_key)['ContentLength']
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
        
if __name__ == "__main__":
    s3 = S3Storage()
    s3.init_s3_storage()
    
    s3.test_s3()
    s3.close()