from kubernetes import client, config
import redis

from database import Database
from storage import S3Storage


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
    

if __name__ == "__main__":
    s3 = S3Storage()
    s3.init_s3_storage()
    s3.test_s3()
    s3.close()
    
    # db = Database()
    # db.init_database()
    # db.get_existing_tables()
    # db.test_db()
    # db.close()
    # spawn_test_pods(3)
    