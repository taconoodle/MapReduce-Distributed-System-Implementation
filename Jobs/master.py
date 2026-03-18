from Manager.database import Database
from Manager.task_scheduler import Redis
from Manager.storage import S3Storage

from filesplit.split import Split

class Master:
    def __init__(self, job_id=None):
        self.job_id = job_id
        self.job_filepath = 'Jobs/2mb.pdf'
        self.db = Database()
        self.s3 = S3Storage()
        self.redis = Redis()
        
    # def complete_job():
    #     return
    
    
    def get_job_details(self):
        search_query = '''
            SELECT filepath FROM job_metadata.jobs
            WHERE job_id = %s;
        '''
        
        with self.db.conn.cursor() as cur:
            cur.execute(search_query, (self.job_id))
            self.job_filepath = cur.fetchone()
        
        self.db.close()
        return self.job_filepath
    
    
    def split_initial_file(self):
        split = Split(self.job_filepath, 'intermediate_files')
        split.bysize(size=64000, callback=INSERT FUNCTION HERE)
    
    
    def handle_file_chunk(self, chunkpath, chunksize):
        # TODO: Put in bucket
        self. s3.add_to_bucket(f'jobs/job_{self.job_id}/intermediate_files', chunkpath, )
        # TODO: Push to Redis
    
    
    def run(self):
        print(f'Master starting job with ID {self.job_id}')
        self.db.init_database()
        self.s3.init_s3_storage()
        self.redis.initialize() # TODO
            
        self.get_job_details()
        self.s3.create_bucket(f'jobs/job_{self.job_id}/intermediate_files')
        self.redis.create_task_queue() # TODO

        self.split_initial_file()
        self.execute_workers()
        
        self.complete_job()
        
    
if __name__ == "__main__":
    master = Master()
    master.split_initial_file()