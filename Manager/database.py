import psycopg2
from psycopg2 import sql
import logging

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
        
   
        queries = [
            "CREATE SCHEMA IF NOT EXISTS job_metadata;",
            """CREATE TABLE IF NOT EXISTS job_metadata.jobs (
                job_id UUID PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                input_file_name VARCHAR(255),
                output_file_name VARCHAR(255),
                job_status VARCHAR(20) DEFAULT 'Pending',
                total_chunks INTEGER DEFAULT 0,
                current_phase_completed_tasks INTEGER DEFAULT 0,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );""",
            """CREATE TABLE IF NOT EXISTS job_metadata.tasks (
                job_id UUID REFERENCES job_metadata.jobs(job_id),
                task_id INTEGER,
                task_type VARCHAR(20),
                task_status VARCHAR(20),
                PRIMARY KEY (job_id, task_id)
            );"""
        ]
        
        with self.conn.cursor() as cur:
            for q in queries:
                cur.execute(q)
            self.conn.commit()


    def insert_job(self, job_id, user_id, input_filename, output_filename):
        query = """
            INSERT INTO job_metadata.jobs (job_id, user_id, input_file_name, output_file_name)
            VALUES (%s, %s, %s, %s);
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (job_id, user_id, input_filename, output_filename))
            self.conn.commit()

    def update_job_status(self, job_id, status):
        query = "UPDATE job_metadata.jobs SET job_status = %s WHERE job_id = %s;"
        with self.conn.cursor() as cur:
            cur.execute(query, (status, job_id))
            self.conn.commit()

    def update_job_chunks(self, job_id, total):
        query = "UPDATE job_metadata.jobs SET total_chunks = %s WHERE job_id = %s;"
        with self.conn.cursor() as cur:
            cur.execute(query, (total, job_id))
            self.conn.commit()

    def increment_and_fetch_counters(self, job_id):
        query = """
            UPDATE job_metadata.jobs
            SET current_phase_completed_tasks = current_phase_completed_tasks + 1
            WHERE job_id = %s
            RETURNING current_phase_completed_tasks, total_chunks, job_status;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (job_id,))
            res = cur.fetchone()
            self.conn.commit()
            return res

    def reset_phase_counter(self, job_id):
        query = "UPDATE job_metadata.jobs SET current_phase_completed_tasks = 0 WHERE job_id = %s;"
        with self.conn.cursor() as cur:
            cur.execute(query, (job_id,))
            self.conn.commit()

    def get_job_info(self, job_id, field="job_status"):
        query = sql.SQL("SELECT {} FROM job_metadata.jobs WHERE job_id = %s;").format(sql.Identifier(field))
        with self.conn.cursor() as cur:
            cur.execute(query, (job_id,))
            res = cur.fetchone()
            return res[0] if res else None

    def get_active_jobs(self, statuses):
        query = "SELECT job_id FROM job_metadata.jobs WHERE job_status IN %s;"
        with self.conn.cursor() as cur:
            cur.execute(query, (tuple(statuses),))
            return [row[0] for row in cur.fetchall()]


    def insert_task(self, job_id, task_id, task_type, status):
        query = "INSERT INTO job_metadata.tasks (job_id, task_id, task_type, task_status) VALUES (%s, %s, %s, %s);"
        with self.conn.cursor() as cur:
            cur.execute(query, (job_id, task_id, task_type, status))
            self.conn.commit()

    def get_task_status(self, job_id, task_id):
        query = "SELECT task_status FROM job_metadata.tasks WHERE job_id = %s AND task_id = %s;"
        with self.conn.cursor() as cur:
            cur.execute(query, (job_id, task_id))
            res = cur.fetchone()
            return res[0] if res else None

    def update_task_status(self, job_id, task_id, status):
        query = "UPDATE job_metadata.tasks SET task_status = %s WHERE job_id = %s AND task_id = %s;"
        with self.conn.cursor() as cur:
            cur.execute(query, (status, job_id, task_id))
            self.conn.commit()

    def delete_all_tasks(self, job_id):
        query = "DELETE FROM job_metadata.tasks WHERE job_id = %s;"
        with self.conn.cursor() as cur:
            cur.execute(query, (job_id,))
            self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()