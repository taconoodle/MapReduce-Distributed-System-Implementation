import asyncio
import hashlib
import logging
import os
from contextlib import asynccontextmanager
from enum import Enum
import threading


from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from kubernetes import client, config, watch
from filesplit.split import Split
from pydantic import BaseModel
from Manager.database import Database
from Manager.storage import S3Storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BACKOFF_LIMIT = 3
NAMESPACE     = "default"

REDUCER_COUNT = 4
DATA_BUCKET = 'rustfs'

# Enums
class JobStatus(str, Enum):
    PENDING  = "Pending"
    MAP      = "Map"
    SHUFFLE  = "Shuffle"
    REDUCE   = "Reduce"
    SUCCEED  = "Succeed"
    FAILED   = "Failed"

class TaskType(str, Enum):
    MAP     = "Map"
    SHUFFLE = "Shuffle"
    REDUCE  = "Reduce"

class TaskStatus(str, Enum):
    PENDING   = "Pending"
    COMPLETED = "Completed"
    FAILED    = "Failed" #we will see if this necessary

class Manager:
    def __init__(self, replica_id):
        self.replica_id = replica_id
        self.db  = Database()
        self.sfs = S3Storage()
        #self.watchers: dict[str, asyncio.Task] = {} remove this 

    async def init_services(self):
        self.db.init_database()
        self.sfs.init_s3()
        

    async def startup(self):
        logger.info(f"[Manager {self.replica_id}] Starting up...")

        await self.init_services()
        
        await self.recover_from_crash()

        #await self.restart_active_watchers()
        asyncio.create_task(self.watch_all())

        logger.info(f"[Manager {self.replica_id}] Startup complete and listening.")

    async def recover_from_crash(self):
        # Statuses considered "active" — jobs in these stages
        
        active_statuses = [JobStatus.MAP, JobStatus.SHUFFLE, JobStatus.REDUCE]
        active_jobs = self.db.get_active_jobs(active_statuses)

        # If no active jobs exist, no action is needed.
        if not active_jobs:
            logger.info(f"[Manager {self.replica_id}] No active jobs found in DB to recover.")
            return

        batch = client.BatchV1Api()

        for job_db in active_jobs:
            job_id = job_db['job_id']
            try:
                # Fetch only the K8s jobs that belong to this job
                # and are managed by this Manager replica.
                selector = f"job_id={job_id},manager_id={self.replica_id}"
                k8s_jobs = batch.list_namespaced_job(namespace=NAMESPACE, label_selector=selector)

                # Tasks for this job as known by the database.
                tasks = self.db.get_tasks_for_job(job_id)

                # Set of task_ids that have a corresponding K8s job — used
                # below to detect orphaned tasks.
                active_k8s_task_ids = {
                    k8s_job.metadata.labels.get("task_id") for k8s_job in k8s_jobs.items
                }

                # If no K8s jobs exist for this job, the Manager crashed before
                # it could create or monitor the pods. If one Non completed mark the job as Failed
                if not k8s_jobs.items:
                    logger.warning(f"[Recover] Job {job_id} has no running Kubernetes Jobs.")
                    for task in tasks:
                        if task['status'] != TaskStatus.COMPLETED:
                            logger.info(f"Orphaned task {task['task_id']} for job {job_id} and failed job")
                            self.db.update_task_status(job_id, task['task_id'], TaskStatus.FAILED)
                            await self.handle_job_failure(job_id)
                            break
                            
                    continue

                # Inspect each K8s job and decide what action to take
                # based on its current status.
                for k8s_job in k8s_jobs.items:
                    task_id    = k8s_job.metadata.labels.get("task_id")
                    task_type  = k8s_job.metadata.labels.get("task_type")
                    worker_num = int(k8s_job.metadata.labels.get("worker_num"))

                    # Fall back to default values if spec fields are None.
                    backoff_limit = k8s_job.spec.backoff_limit if k8s_job.spec.backoff_limit is not None else 3
                    completions   = k8s_job.spec.completions  if k8s_job.spec.completions  is not None else 1

                    if k8s_job.status.failed is not None and k8s_job.status.failed >= backoff_limit:
                        # The task has exhausted all retries — permanent failure.
                        # Update the database and invoke the failure handler,
                        # which will stop the entire job.
                        logger.warning(f"[Recover] Job {job_id} task {task_id} found FAILED.")
                        self.db.update_task_status(job_id, task_id, TaskStatus.FAILED)
                        #self.db.update_job_status(job_id, JobStatus.FAILED)
                        await self.handle_job_failure(job_id)
                        break

                    elif k8s_job.status.succeeded is not None and k8s_job.status.succeeded >= completions:
                        # The task completed successfully in K8s. Check whether
                        # the database already reflects this, to avoid double
                        # processing in case the handler had finished before the crash.
                        if self.db.get_task_status(job_id, task_id) != TaskStatus.COMPLETED:
                            logger.info(f"[Recover] Job {job_id} task {task_id} found SUCCEEDED.")
                            #self.db.update_task_status(job_id, task_id, TaskStatus.COMPLETED)
                            await self.handle_task_succeeded(job_id, task_id, task_type, worker_num)

                    else:
                        # The task is still running normally — the watcher will
                        # continue monitoring it after recovery.
                        logger.info(f"[Recover] Job {job_id} task {task_id} is still running in K8s. The watcher will monitor it.")

                # Detect orphaned tasks: present in the database but missing a
                # corresponding K8s job — likely lost during the crash.
                # If one failed marked job as Failed
                for task in tasks:
                    if task['task_id'] not in active_k8s_task_ids and task['status'] != TaskStatus.COMPLETED:
                        logger.warning(f"[Reconcile] Task {task['task_id']} is missing from Kubernetes. Rescheduling...")
                        self.db.update_task_status(job_id, task['task_id'], TaskStatus.FAILED)
                        await self.handle_job_failure(job_id)
                        break
            except Exception as e:
                # If recover fails for one job, log the error
                # and continue processing the remaining jobs.
                logger.error(f"[Recover] Error checking job {job_id}: {e}")

        logger.info(f"[Manager {self.replica_id}] Recover complete.")
    
    async def watch_all(self):
        # Grab the running event loop so that coroutines scheduled from the
        # blocking thread can be dispatched back onto it via run_coroutine_threadsafe.
        loop = asyncio.get_running_loop()
        # Shared flag used to signal the blocking thread to stop gracefully
        # when the async task is cancelled.
        #stop_flag = False
        stop_event = threading.Event()


        def blocking_watch():
            batch = client.BatchV1Api()
            w = watch.Watch()

            # Only watch K8s jobs that belong to this Manager replica.
            selector = f"manager_id={self.replica_id}"

            logger.info(f"[Manager replica={self.replica_id}] Watcher started.")
            while not stop_event.is_set():
                try:
                    for event in w.stream(batch.list_namespaced_job, namespace=NAMESPACE, label_selector=selector):

                        if stop_event.is_set():
                            w.stop()
                            break

                        k8s_job    = event["object"]
                        event_type = event["type"]

                        # Safely extract labels, defaulting to an empty dict if None.
                        labels     = k8s_job.metadata.labels or {}
                        job_id     = labels.get("job_id")
                        task_id    = labels.get("task_id")
                        task_type  = labels.get("task_type")
                        worker_num = int(labels.get("worker_num"))


                        # Skip all events for jobs that have already been marked as FAILED —
                        # no further processing is meaningful for a dead job.
                        if self.db.get_job_status(job_id) == JobStatus.FAILED:
                            continue

                        # Ignore any event types outside the three we handle.
                        if event_type not in ("ADDED", "MODIFIED", "DELETED"):
                            continue

                        if event_type == "DELETED":
                            logger.warning(f"[Manager replica={self.replica_id}] Job {job_id} task {task_id} was deleted.")

                            # If the task was deleted before completing, mark it as FAILED
                            # If it was already COMPLETED,
                            # the deletion is expected and we do nothing.
                            current_task_status = self.db.get_task_status(job_id, task_id)
                            if current_task_status != TaskStatus.COMPLETED:
                                logger.info(f"[Manager replica={self.replica_id}] Task {task_id} did not complete successfully. Rescheduling...")
                                self.db.update_task_status(job_id, task_id, TaskStatus.FAILED)
                                asyncio.run_coroutine_threadsafe(self.handle_job_failure(job_id), loop)
                            continue

                        # Fall back to default values if spec fields are None.
                        backoff_limit = k8s_job.spec.backoff_limit if k8s_job.spec.backoff_limit is not None else 3
                        completions   = k8s_job.spec.completions  if k8s_job.spec.completions  is not None else 1

                        if k8s_job.status.failed is not None and k8s_job.status.failed >= backoff_limit:
                            # The task has exhausted all retries — permanent failure.
                            # Update the database and schedule the failure handler on the
                            # event loop, since we are inside a blocking thread.
                            logger.error(f"[Manager replica={self.replica_id}] job={job_id} task={task_id} failed permanently.")
                            self.db.update_task_status(job_id, task_id, TaskStatus.FAILED)
                            #self.db.update_job_status(job_id, JobStatus.FAILED)
                            asyncio.run_coroutine_threadsafe(
                                self.handle_job_failure(job_id), loop
                            )

                        elif k8s_job.status.succeeded is not None and k8s_job.status.succeeded >= completions:
                            # The task completed successfully.
                            # in case a previous event already marked it as COMPLETED.
                            if self.db.get_task_status(job_id, task_id) != TaskStatus.COMPLETED:
                                logger.info(f"[Manager replica={self.replica_id}] job={job_id} task={task_id} succeeded.")
                                #self.db.update_task_status(job_id, task_id, TaskStatus.COMPLETED)
                                asyncio.run_coroutine_threadsafe(
                                    self.handle_task_succeeded(job_id, task_id, task_type, worker_num), loop
                                )

                        elif k8s_job.status.failed is not None and k8s_job.status.failed > 0:
                            # A pod attempt failed but retries remain — K8s will restart it
                            # automatically. Update the database to reflect the transient failure.
                            logger.warning(f"[Manager replica={self.replica_id}] job={job_id} task={task_id} failed a pod attempt. Retrying in K8s...")
                            self.db.update_task_status(job_id, task_id, TaskStatus.FAILED)
        
                except Exception as e:
                    logger.error(f"Watcher error: {e}")
                    stop_event.wait(5) 
        try:
            # Run the blocking watch loop in a thread pool executor so it does
            # not block the async event loop.
            await loop.run_in_executor(None, blocking_watch)
        except asyncio.CancelledError:
            # Signal the blocking thread to exit on the next iteration.
            stop_event.set()
            logger.info(f"[Manager replica={self.replica_id}] Watcher cancelled.")
            raise
        finally:
            logger.info(f"[Manager replica={self.replica_id}] Watcher exited.")
    
    async def handle_task_succeeded(self,job_id, task_id, task_type, worker_num):
        if self.db.get_task_status(job_id, task_id) == TaskStatus.COMPLETED:
            return
        self.db.update_task_status(job_id,task_id,TaskStatus.COMPLETED)
        completed=self.db.increment_and_fetch_counters(job_id)
        total=worker_num

        #The old version
        
        #result=self.db.increment_and_fetch_counters(job_id)
        #completed,total_chunks,job_status=result
        #total = total_chunks if job_status == "Map" else REDUCER_COUNT
        

        logger.info(f"[Handler] job={job_id} {task_type} {task_id} ({completed}/{total})")

        if completed == total:
            if task_type == "Map":
                self.db.reset_phase_counter(job_id)
                #transition to shuffle
                #spawn shufflers
                

            elif task_type == "Shuffle":
                self.db.reset_phase_counter(job_id)
                #transition to reduce 
                #spawn reducers
                

            elif task_type == "Reduce":
                #finalize the job
                #notify UI
                return

    async def handle_job_failure(self,job_id):
        self.db.update_job_status(job_id,JobStatus.FAILED)
        self.db.delete_all_tasks(job_id)
        await self.send_cancel_signal_to_workers(job_id)
        #notify UI

        #clean all the buckets from sfs(a try)
        prefix = f"jobs/{job_id}/"
        deleted_keys=list(self.sfs.stream_keys_in_dir(DATA_BUCKET, prefix))
        if not deleted_keys:
            return
        
        for key in deleted_keys:
            self.sfs.delete_from_bucket(DATA_BUCKET,key)            
        return

    async def verify_token(token: str = Depends()):#inside Depends something on Keycloak
        return


    #something like this for verify Market Task
    """
    @staticmethod
    def verify_token(token: str) -> UserInfo:
        try:
            user_info = keycloak_openid.userinfo(token)
            print(user_info)
            if not user_info:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
                )
            return UserInfo(
                preferred_username=user_info["preferred_username"],
                email=user_info.get("email"),
                full_name=user_info.get("name"),
                #realm_access=user_info.get("realm_access")
            )
        except KeycloakAuthenticationError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
            )
    """


    #app = FastAPI()

    #@app.post("/cancel-job/{job_id}")
    async def cancel_job(self,job_id,token_data: dict = Depends(verify_token)):
       
        success = await self.send_cancel_signal_to_workers(job_id)

        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Job {job_id} not found or already completed",)

        #Maybe this
        #tasks = self.db.get_tasks_for_job(job_id)
        #for task in tasks:
        #   task_id = task["task_id"]
        #   self.db.update_task_status(task_id,TaskStatus.FAILED)
        #asyncio.run_coroutine_threadsafe(self.handle_job_failure(job_id,self.replica_id))
        return {"status": "cancelled", "job_id": job_id}


    async def send_cancel_signal_to_workers(self,job_id):
        batch_v1 = client.BatchV1Api()
        try:
            batch_v1.delete_collection_namespaced_job(namespace=NAMESPACE,label_selector=f"job-id={job_id}")
        except client.exceptions.ApiException as e:
            print(f"Exception when calling BatchV1Api delete_namespaced_job: {e}")
            return False



    """
    async def transition_to_shuffle(self, job_id):
        self.db.update_job_status(job_id, JobStatus.SHUFFLE.value)
        self.db.reset_phase_counter(job_id)
        self.spawn_workers(job_id, TaskType.SHUFFLE, REDUCER_COUNT, image="shuffle-worker:latest")

    async def transition_to_reduce(self, job_id):
        self.db.update_job_status(job_id, JobStatus.REDUCE.value)
        self.db.reset_phase_counter(job_id)
        self.spawn_workers(job_id, TaskType.REDUCE, REDUCER_COUNT, image="reduce-worker:latest")




    #check this for output prefix Πήρα τις μεθόδους του storage από το δικό σου
    async def finalize_job(self, job_id):
        
        out_file_name = self.db.get_job_info(job_id, "output_file_name")
        
        final_output_path = f"jobs/{job_id}/output_files/job_{job_id}.json"
        
        self.db.update_job_status(job_id, JobStatus.SUCCEED.value)


        intermediate_prefix = f"jobs/{job_id}/intermediate_files/"
        try:
            for key in self.sfs.stream_keys_in_dir(DATA_BUCKET, intermediate_prefix):
                self.sfs.delete_from_bucket(DATA_BUCKET, key)
            logger.info(f"[Job {job_id}] Intermediate storage cleared.")
        except Exception as e:
            logger.warning(f"Cleanup failed for job {job_id}: {e}")

        self.delete_workers(job_id) 
        self.db.delete_all_tasks(job_id) 
        self.stop_watcher(job_id)
        logger.info(f"[Job {job_id}] Finalization complete.")

    
    

    def spawn_workers(self, job_id, phase, count, image):
        config.load_kube_config()
        batch_v1 = client.BatchV1Api()
        
        for i in range(1, count + 1):
            worker_name = f"job-{job_id}-{phase.value.lower()}-{i}"
            
            self.db.insert_task(job_id, i, phase.value, TaskStatus.PENDING.value)
            
            container = client.V1Container(
                name="worker",
                image=image,
                env=[
                    client.V1EnvVar(name="JOB_ID", value=str(job_id)),
                    client.V1EnvVar(name="WORKER_ID", value=str(i)),
                    client.V1EnvVar(name="WORKER_NUM", value=str(count)),
                ]
            )
            
            template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={
                    "manager_id": self.replica_id,
                    "job_id": str(job_id),
                    "task_id": str(i),
                    "phase": phase.value
                }),
                spec=client.V1PodSpec(containers=[container], restart_policy="Never")
            )
            
            k8s_job = client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=client.V1ObjectMeta(name=worker_name),
                spec=client.V1JobSpec(template=template, backoff_limit=0)
            )
            
            batch_v1.create_namespaced_job(namespace="default", body=k8s_job)

    def delete_workers(self, job_id):
        config.load_kube_config()
        batch_v1 = client.BatchV1Api()
        
        jobs = batch_v1.list_namespaced_job(namespace="default")
        for job in jobs.items:
            if str(job_id) in job.metadata.name:
                try:
                    batch_v1.delete_namespaced_job(
                        name=job.metadata.name,
                        namespace="default",
                        body=client.V1DeleteOptions(propagation_policy="Foreground")
                    )
                except: pass

                

     async def restart_active_watchers(self):
        active_statuses = [JobStatus.MAP.value, JobStatus.REDUCE.value]
        active_jobs = self.db.get_active_jobs(active_statuses)
        
        for job_id in active_jobs:
            logger.info(f"[Startup] Re-arming watcher for active job {job_id}")
            self.start_watcher(job_id)

    async def recover_from_crash(self):
        config.load_kube_config()
        v1 = client.CoreV1Api()
        
        label_selector = f"manager_id={self.replica_id}"
        pods = v1.list_namespaced_pod(namespace="default", label_selector=label_selector)

        for pod in pods.items:
            job_id = pod.metadata.labels.get("job_id")
            task_id = pod.metadata.labels.get("task_id")
            pod_phase = pod.status.phase  # 'Succeeded', 'Failed', 'Running'
            
            if not job_id or not task_id:
                continue

            db_status = self.db.get_task_status(job_id, task_id)

            if db_status != TaskStatus.COMPLETED:
                if pod_phase == "Succeeded":
                    logger.info(f"[Recovery] Task {task_id} of Job {job_id} found Succeeded.")
                    await self.handle_task_succeeded(job_id, task_id)
                elif pod_phase == "Failed":
                    logger.error(f"[Recovery] Task {task_id} of Job {job_id} found Failed.")
                    await self.handle_job_failure(job_id, pod.metadata.name)

   
   
    """    
