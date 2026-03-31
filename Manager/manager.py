from kubernetes import client, config

from database import Database
from storage import S3Storage


REDIS_USERNAME = 'admin'
REDIS_PASSWORD = 'admin'

KEYCLOAK_USERNAME = 'admin'
KEYCLOAK_PASSWORD = 'admin'

class Manager:
    def __init__():
        return
        
    def run():
        # Initalize and configure connections with database and storage
        # Check if there are jobs for you in the DB
        # If there are get pods' status with K8s API and compare it to tasks in DB. Make the necessary changes
        # Create a watcher for the workers. It will run asynchronously
        
        # Initialize a FastAPI server
        # Start listening for requests
        
        exit(0)
        
        
    def spawn_test_pods(count):
        # config.load_incluster_config() # Use this when running from within the cluster
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
    