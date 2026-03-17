import psycopg2
from psycopg2 import sql


POSTGRES_USERNAME = 'admin'
POSTGRES_PASSWORD = 'admin'
POSTGRES_HOST_URL = 'localhost'


class Database:
    def __init__(self):
        self.conn = None
        
        
    def init_database(self):
        if self.conn is not None and not self.conn.closed:
            return self.conn
        
        self.conn = psycopg2.connect(
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
        
        
        with self.conn.cursor() as cur:
            cur.execute(create_schemata_query)
            cur.execute(create_table_query)
            self.conn.commit()
            
        return self.conn
    
    
    def close(self):
        if self.conn:
            self.conn.close()
            
            
    def add_job_to_db(self, user_id, filepath):
        insert_job_query = '''
            INSERT INTO jobs (user_id, filepath)
            VALUES (%s, %s)
            RETURNING job_id;
        '''
        
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO job_metadata;")
            cur.execute(insert_job_query, (user_id, filepath))
            
            new_job_id = cur.fetchone()
            self.conn.commit()
            
        print(f'Added new job with ID {new_job_id} in database')
        return new_job_id
    
    
    def update_job_status(self, job_uuid, status):
        update_query = '''
            UPDATE jobs
            SET status = %s
            WHERE job_id = %s;
        '''
        
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO job_metadata;")
            cur.execute(update_query, (status, job_uuid))
            self.conn.commit()    
        
        return
    
    
    def get_all_jobs(self):
        select_query = '''
            SELECT * FROM jobs
            ORDER BY creation_time DESC;
        '''
        
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO job_metadata;")
            cur.execute(select_query)
            
            jobs = cur.fetchall()
            
        return jobs


    def get_pending_jobs(self):
        select_query = '''
            SELECT * FROM jobs
            WHERE status = 'PENDING'
            ORDER BY creation_time DESC;
        '''
        
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO job_metadata")
            cur.execute(select_query)
            
            jobs = cur.fetchall()
            
        return jobs
    
    
    def delete_job(self, job_uuid):
        delete_query = '''
            DELETE FROM jobs
            WHERE job_id = %s;
        '''
        
        with self.conn.cursor() as cur:
            cur.execute("SET search_path TO job_metadata;")
            cur.execute(delete_query, (job_uuid))
            
            self.conn.commit()
            
        return
    
    
    def get_existing_tables(self):
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'job_metadata'
            """)

            tables = cursor.fetchall()

            print("Existing Tables:")
            for table in tables:
                print(f"- {table[0]}")
                

    def drop_table(self, schema_name, table_name):
        drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{};").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        )
                            
        with self.conn.cursor() as cur:
            cur.execute(drop_query)
            self.conn.commit()
            
            
    def test_db(self):    
        self.get_existing_tables()
        
        job_id = self.add_job_to_db(23, 'test.txt')
        
        print('Printing pending jobs:')
        for job in self.get_pending_jobs():
            print(f"ID: {job[0]} | User: {job[1]} | Status: {job[2]} | Path: {job[3]} | Created: {job[4]}")
            
        self.update_job_status(job_id, 'COMPLETED')
        
        print('Printing all jobs:')
        for job in self.get_all_jobs():
            print(f"ID: {job[0]} | User: {job[1]} | Status: {job[2]} | Path: {job[3]} | Created: {job[4]}")
        
        self.delete_job(job_id)
        
        print('Printing all jobs:')
        for job in self.get_all_jobs():
            print(f"ID: {job[0]} | User: {job[1]} | Status: {job[2]} | Path: {job[3]} | Created: {job[4]}")
            
        return
    
    
if __name__ == "__main__":
    db = Database()
    db.init_database()
    
    db.test_db()
    db.close()