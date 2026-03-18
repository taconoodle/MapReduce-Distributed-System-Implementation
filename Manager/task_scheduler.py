import redis
from redis.exceptions import ConnectionError, TimeoutError

REDIS_HOST = 'localhost' 
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = 'admin'

class Redis:
    def __init__(self):
        self.pool = None
        self.conn = None
        
    def init_redis(self):
        try:
            self.pool = redis.ConnectionPool(#with pool can handle more connections
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_timeout=5#The time which python waits redis to response 
            )
            self.conn = redis.Redis(connection_pool=self.pool)
            self.conn.ping()
            return self.conn
        except ConnectionError as e:
            print(f"Connection error: {e}")
            return None

    def add_task_to_queue(self, job_id, filepath, task_type="map"):#we will see if map is important
        pipe = self.conn.pipeline()
        try:
            pipe.incr(f"job:{job_id}:next_task_id")#increase by one
            res = pipe.execute()
            task_num = res[0]
            
            task_id = f"{job_id}.{task_num}" 
            task_key = f"task:{task_id}"
            
            pipe.hset(task_key, mapping={#create a hash
                "job_id": job_id,
                "filepath": filepath,
                "status": "pending",
                "type": task_type 
            })
            
            pipe.sadd(f"job:{job_id}:tasks", task_id)#job_id->task_id (set)
            pipe.lpush("task_queue", task_id)
            pipe.execute()
            return task_id
        
        except Exception as e:
            print(f"Failed to add task: {e}")
            return None

    def get_next_task(self, timeout=0):
        try:
            res = self.conn.brpop("task_queue", timeout=timeout)#The brpop return a tuple
            if res:
                task_id = res[1]
                self.conn.hset(f"task:{task_id}", "status", "running")
                task_data = self.conn.hgetall(f"task:{task_id}")#The hgetall return all the value we store
                task_data['task_id'] = task_id
                return task_data
        except (ConnectionError, TimeoutError):
            print("Connection with redis is gone.")
        return None
    
    def complete_task(self, job_id, task_id, result_path):
        task_key = f"task:{task_id}"
        pipe = self.conn.pipeline()
        pipe.hset(task_key, "status", "completed")
        pipe.hset(task_key, "result_path", result_path)
        pipe.incr(f"job:{job_id}:completed_count")
        pipe.execute()

    def check_job_status(self, job_id):
        total = self.conn.get(f"job:{job_id}:next_task_id")#Return the value from one key
        completed = self.conn.get(f"job:{job_id}:completed_count")
        total_tasks = int(total) if total else 0
        completed_tasks = int(completed) if completed else 0
        
        return {
            "total": total_tasks,
            "completed": completed_tasks,
            "done": completed_tasks >= total_tasks and total_tasks > 0
        }

    def delete_task(self, job_id, task_id):
        pipe = self.conn.pipeline()
        pipe.delete(f"task:{task_id}")
        pipe.srem(f"job:{job_id}:tasks", task_id)#Remove one specific value from one set
        pipe.lrem("task_queue", 0, task_id)#Αφαιρεί στοιχεία από μια λίστα και με το 0 αφαιρεί όλες τις εμφανίσεις του task id.
        pipe.execute()
        print(f"Task {task_id} deleted.")


    def delete_entire_job(self, job_id):
        tasks_set_key = f"job:{job_id}:tasks"
        task_ids = self.conn.smembers(tasks_set_key)#Return all the values from one set
        
        pipe = self.conn.pipeline()
        for tid in task_ids:
            pipe.delete(f"task:{tid}")
            pipe.lrem("task_queue", 0, tid)
        
        pipe.delete(tasks_set_key)
        pipe.delete(f"job:{job_id}:next_task_id")
        pipe.delete(f"job:{job_id}:completed_count")
        pipe.execute()
        print(f"Job {job_id} cleared.")

    def close(self):
        if self.pool:
            self.pool.disconnect()