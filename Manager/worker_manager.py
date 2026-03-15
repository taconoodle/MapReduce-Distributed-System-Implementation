from kubernetes import client, config
import redis
import psycopg2
import boto3
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

def manager_init():
    db = psycopg2.connect(
        host=POSTGRES_HOST_URL,
        port=5432,
        dbname='distributed_db',
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD
    )
    
    s3 = boto3.client(
        's3',
        endpoint_url=RUSTFS_URL,
        aws_access_key_id=RUSTFS_USERNAME,
        aws_secret_access_key=RUSTFS_PASSWORD,
        config=Config(signature_version='s3v4'), # The version of the signatures the authenticates AWS requests. RustFS wants s3v4
        region_name='us-east-1' # Apparently RustFS expects us to use us-east-1
    )
    
    s3.list_buckets()
    
def add_job_to_db():
    return

# spawn_test_pods(3)
manager_init()
