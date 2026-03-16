from kubernetes import client, config
import redis
import psycopg2
from psycopg2 import sql
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
    
    create_schemata_query = '''
        CREATE SCHEMA IF NOT EXISTS job_metadata;
        CREATE SCHEMA IF NOT EXISTS auth_metadata;
    '''
    
    
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS job_metadata.jobs (
            job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(50) NOT NULL,
            status VARCHAR(20) DEFAULT 'PENDING',
            filepath VARCHAR(128) NOT NULL,
            creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    '''
    
    
    with db.cursor() as cur:
        cur.execute(create_schemata_query)
        cur.execute(create_table_query)
        db.commit()
        
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
    
    
def add_job_to_db(db, user_id, filepath):

    insert_job_query = '''
        INSERT INTO jobs (user_id, filepath)
        VALUES (%s, %s)
        RETURNING job_id;
    '''
    
    with db.cursor() as cur:
        cur.execute("SET search_path TO job_metadata;")
        cur.execute(insert_job_query, (user_id, filepath))
        
        new_job_id = cur.fetchone()
        db.commit()
        
    print(f'Added new job with ID {new_job_id} in database')
    return new_job_id


def update_job_status(db, job_uuid, status):
    update_query = '''
        UPDATE jobs
        SET status = %s
        WHERE job_id = %s;
    '''
    
    with db.cursor() as cur:
        cur.execute("SET search_path TO job_metadata;")
        cur.execute(update_query, (status, job_uuid))
        db.commit()    
    
    return


def get_all_jobs(db):
    select_query = '''
        SELECT * FROM jobs
        ORDER BY creation_time DESC;
    '''
    
    with db.cursor() as cur:
        cur.execute("SET search_path TO job_metadata;")
        cur.execute(select_query)
        
        jobs = cur.fetchall()
        
    return jobs


def get_pending_jobs(db):
    select_query = '''
        SELECT * FROM jobs
        WHERE status = 'PENDING'
        ORDER BY creation_time DESC;
    '''
    
    with db.cursor() as cur:
        cur.execute("SET search_path TO job_metadata")
        cur.execute(select_query)
        
        jobs = cur.fetchall()
        
    return jobs
    

def delete_job(db, job_uuid):
    delete_query = '''
        DELETE FROM jobs
        WHERE job_id = %s;
    '''
    
    with db.cursor() as cur:
        cur.execute("SET search_path TO job_metadata;")
        cur.execute(delete_query, (job_uuid))
        
        db.commit()
        
    return


def get_existing_tables(db):
    with db.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'job_metadata'
        """)

        tables = cursor.fetchall()

        print("Existing Tables:")
        for table in tables:
            print(f"- {table[0]}")
            

def drop_table(db, schema_name, table_name):
    drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{};").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name)
    )
                         
    with db.cursor() as cur:
        cur.execute(drop_query)
        db.commit()


def test_db(db):    
    
    job_id = add_job_to_db(db, 23, 'test.txt')
    
    print('Printing pending jobs:')
    for job in get_pending_jobs(db):
        print(f"ID: {job[0]} | User: {job[1]} | Status: {job[2]} | Path: {job[3]} | Created: {job[4]}")
        
    update_job_status(db, job_id, 'COMPLETED')
    
    print('Printing all jobs:')
    for job in get_all_jobs(db):
        print(f"ID: {job[0]} | User: {job[1]} | Status: {job[2]} | Path: {job[3]} | Created: {job[4]}")
    
    delete_job(db, job_id)
    
    print('Printing all jobs:')
    for job in get_all_jobs(db):
        print(f"ID: {job[0]} | User: {job[1]} | Status: {job[2]} | Path: {job[3]} | Created: {job[4]}")
        
    return

if __name__ == "__main__":
    # s3 = init_s3_storage()
    # test_s3(s3)
    
    db = init_database()
    get_existing_tables(db)
    # drop_table(db, 'job_metadata', 'jobs')
    # get_existing_tables(db)
    test_db(db)
    
    db.close()
    # spawn_test_pods(3)
    