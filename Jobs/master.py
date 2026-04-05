from Manager.database import Database
from Manager.task_scheduler import Redis
from Manager.storage import S3Storage

# from filesplit.split import Split

class Master:
    def __init__(self, job_id=None):
        # self.job_id = job_id
        # self.job_filepath = 'test.txt'
        # self.db = Database()
        # self.s3 = S3Storage()
        # self.redis = Redis()

        # Master will know the input file, the output file and its own Job ID

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
        split.bysize(size=64000, newline=True, callback=self.handle_file_chunk)
        # split.bysize(size=64000, newline=True)

    
    def handle_file_chunk(self, chunkpath, chunksize):
        # TODO: Put in bucket
        self.s3.add_to_bucket(f'jobs/job_{self.job_id}/intermediate_files', chunkpath)
        # TODO: Push to Redis
        

    def watch_worker_jobs():
        # Create a Kuberenetes watch object
        # Find the job workers and check if they have failed
        # If they have failed, mark them as dead and mark the Redis task as Failed
        # Possible mark the whole User Job as failed (since if you reach this point the data must be corrupt) and update DB accordingly

        # If they are completed, check if there are any Pending or InProgress tasks in Redis. If there are not, the Map, or Reduce Job is complete and we can move on


    def run(self):
        # print(f'Master starting job with ID {self.job_id}')
        # self.db.init_database()
        # self.s3.init_s3_storage()
        # self.redis.initialize() # TODO

        # self.get_job_details()
        # self.s3.create_bucket(f'jobs/job_{self.job_id}/intermediate_files')
        # self.redis.create_task_queue() # TODO

        # self.split_initial_file()
        # self.execute_workers()

        # self.complete_job()

        # Splits the input file into chunks
        # Creates tasks in Redis for the chunks
        # Creates a folder for each reducer in RustFS
        # Spawns a map worker for each chunk

        # Initialize a watcher to monitor the workers
        # If a worker crashes Kubernetes will automatically restart it up to a number of times
        # If it fails many times K8s will mark it as failed and the watcher will handle it accordingly

        # When a worker is spawned succesfully, marks the Redis task as running (ATTENTION: The worker will probably do this)

        # When a Map Worker finishes, the worker is destroyed and the Redis task deleted (ATTENTION: The worker himself will probably mark the task as completed)

        # Creates the reduce tasks in Redis
        # Spawns a set number of reduce workers
        # When the worker is spawned succesfully, marks the Redis reduce task as running

        # When Reduce workers finish, merges their output files into one
        
        
    
if __name__ == "__main__":
    master = Master()
    master.split_initial_file()