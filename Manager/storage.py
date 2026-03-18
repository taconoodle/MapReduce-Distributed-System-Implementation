import boto3
import botocore
from botocore.client import Config


RUSTFS_USERNAME = 'admin'
RUSTFS_PASSWORD = 'admin'
RUSTFS_URL = 'http://localhost:9000'


class S3Storage:
    
    def __init__(self):
        self.conn = None
        
    
    def init_s3_storage(self):
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
    
    
    def add_to_bucket(self, bucket_name, filename, key=None):
        if key is None:
            key = filename
            
        self.conn.upload_file(Filename=filename, Bucket=bucket_name, Key=key)
    
    
    def get_from_bucket(self, bucket_name, key, filename):
        if filename is None:
            filename = key
            
        self.conn.download_file(Key=key, Bucket=bucket_name, Filename=filename)
        
    
    def remove_from_bucket(self, bucket_name, key):
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
        
        
if __name__ == "__main__":
    s3 = S3Storage()
    s3.init_s3_storage()
    
    s3.test_s3()
    s3.close()