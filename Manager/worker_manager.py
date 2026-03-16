from kubernetes import client, config
import redis
import psycopg2
import boto3
import botocore
from botocore.client import Config

POSTGRES_USERNAME = 'admin'
POSTGRES_PASSWORD = 'admin'
POSTGRES_HOST_URL = 'localhost'

RUSTFS_USERNAME = 'admin'
RUSTFS_PASSWORD = 'admin'
RUSTFS_URL = 'http://localhost:9000'

REDIS_USERNAME = 'admin'
REDIS_PASSWORD = 'admin'

KEYCLOAK_USERNAME = 'admin'
KEYCLOAK_PASSWORD = 'admin'


def spawn_test_pods(count):
    # config.load_incluster_config()
    config.load_kube_config()
    
    container = client.V1Container(
        name="test-worker",
        image="nginx:latest"
    )
    
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "test-worker"}),
        spec=client.V1PodSpec(containers=[container])
    )
    
    spec = client.V1DeploymentSpec(
        selector=client.V1LabelSelector(match_labels={"app": "test-worker"}),
        replicas=count,
        template=template
    )
    
    deployment = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name="test-deployer"),
        spec=spec
    )
    
    v1 = client.AppsV1Api()
    v1.create_namespaced_deployment(namespace="default", body=deployment)

def init_database():
    db = psycopg2.connect(
        host=POSTGRES_HOST_URL,
        port=5432,
        dbname='distributed_db',
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD
    )
    
    return db
    
def init_s3_storage():
    s3 = boto3.client(
        's3',
        endpoint_url=RUSTFS_URL,
        aws_access_key_id=RUSTFS_USERNAME,
        aws_secret_access_key=RUSTFS_PASSWORD,
        config=Config(signature_version='s3v4'), # The version of the signatures the authenticates AWS requests. RustFS wants s3v4
        region_name='us-east-1' # Apparently RustFS expects us to use us-east-1
    )
    
    return s3

def manager_init():
    s3 = init_s3_storage()
    db = init_database()
    
    return s3, db
    
    
def create_bucket(s3_client, bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} created.')
    except s3_client.exceptions.BucketAlreadyExists:
        print(f'Bucket {bucket_name} already exists.')
    except botocore.exceptions.ClientError as e:
        print(f'Unexpected error occured: {e}')
        
 
def test_s3(s3_client):
    bucket_name = 'test-bucket'
    
    create_bucket(s3_client, bucket_name)
    print(f'Created bucket {bucket_name}')
    
    buckets = s3_client.list_buckets()
    print('Buckets found:')
    for bucket in buckets['Buckets']:
        print(f'\t{bucket['Name']}')
        
    s3_client.upload_file(Filename='test.txt', Bucket=bucket_name, Key='test.txt')
    print('File "test.txt" uploaded')
    
    print(f'Now printing objects in {bucket_name}:')
    objects_in_bucket = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in objects_in_bucket.get('Contents', []):
        print(f'\t{obj['Key']} - ({obj['Size']} bytes)')
    
    s3_client.download_file(Bucket=bucket_name, Key='test.txt', Filename='download-test.txt')
    print('File should be have been downloaded as "download-test.txt"')
    
    s3_client.delete_object(Bucket=bucket_name, Key='test.txt')
    print('Object deleted.')

    s3_client.delete_bucket(Bucket=bucket_name)
    print('Bucket deleted.')
    
def add_job_to_db():
    return


if __name__ == "__main__":
    # s3, db = manager_init()
    s3 = init_s3_storage()
    # spawn_test_pods(3)
    
    test_s3(s3)