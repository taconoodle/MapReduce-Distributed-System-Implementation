import asyncio
import logging
import os
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from kubernetes import client, config, watch
from filesplit.split import Split

from database import Database
from storage import S3Storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

class Manager:
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.db  = Database()
        self.sfs = S3Storage()
        self.watchers: dict[str, asyncio.Task] = {}

    
    async def init_services(self):
        self.db.init_database()
        self.sfs.init_s3()

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

    async def restart_active_watchers(self):
        active_statuses = [JobStatus.MAP.value, JobStatus.REDUCE.value]
        active_jobs = self.db.get_active_jobs(active_statuses)
        
        for job_id in active_jobs:
            logger.info(f"[Startup] Re-arming watcher for active job {job_id}")
            self.start_watcher(job_id)
   

    async def startup(self):
        logger.info(f"[Manager {self.replica_id}] Starting up...")

        await self.init_services()

        await self.recover_from_crash()

        await self.restart_active_watchers()

        logger.info(f"[Manager {self.replica_id}] Startup complete and listening.")


    async def handle_task_succeeded(self, job_id, task_id):
        self.db.update_task_status(job_id, task_id, TaskStatus.COMPLETED.value)
        
        completed, total, status_str = self.db.increment_and_fetch_counters(job_id)
        current_status = JobStatus(status_str)
        
        required = total if current_status == JobStatus.MAP else REDUCER_COUNT
        if completed < required: return

        logger.info(f"[Job {job_id}] Phase {current_status} complete.")
        if current_status == JobStatus.MAP:
            await self.transition_to_shuffle(job_id)
        elif current_status == JobStatus.SHUFFLE:
            await self.transition_to_reduce(job_id)
        elif current_status == JobStatus.REDUCE:
            await self.finalize_job(job_id)

    
    
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

    
    async def handle_job_failure(self, job_id, failed_pod):
        self.db.update_job_status(job_id, JobStatus.FAILED.value)
        self.delete_workers(job_id)
        self.db.delete_all_tasks(job_id)
        self.stop_watcher(job_id)

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

    


    async def watch_job(self, job_id):
        config.load_kube_config()
        loop = asyncio.get_event_loop()
        
        stop_flag = {"value": False}

        def blocking_watch():
            v1 = client.CoreV1Api()
            w = watch.Watch()

            selector = f"manager_id={self.replica_id},job_id={job_id}"
            
            logger.info(f"[Watcher {job_id}] Monitoring worker pods...")

            for event in w.stream(v1.list_namespaced_pod, namespace="default", label_selector=selector):
                if stop_flag["value"]:
                    w.stop()
                    break

                pod = event["object"]
                pod_phase = pod.status.phase  # 'Succeeded', 'Failed', 'Running', etc.
                pod_name = pod.metadata.name
                task_id = pod.metadata.labels.get("task_id")

                if event["type"] == "MODIFIED":
                    
                    if pod_phase == "Succeeded":
                        logger.info(f"[Watcher {job_id}] Task {task_id} completed by {pod_name}.")
                        asyncio.run_coroutine_threadsafe(
                            self.handle_task_succeeded(job_id, task_id), loop
                        )

                    elif pod_phase == "Failed":
                        logger.error(f"[Watcher {job_id}] Worker {pod_name} failed. Aborting job.")
                        stop_flag["value"] = True
                        asyncio.run_coroutine_threadsafe(
                            self.handle_job_failure(job_id, pod_name), loop
                        )
                        w.stop()
                        break

        try:
            await loop.run_in_executor(None, blocking_watch)
        except asyncio.CancelledError:
            stop_flag["value"] = True
            logger.info(f"[Watcher {job_id}] Watcher was cancelled.")
        finally:
            logger.info(f"[Watcher {job_id}] Watcher for job {job_id} exited.")

    def start_watcher(self, job_id):
        # check for active watcher for this job
        if job_id in self.watchers and not self.watchers[job_id].done():
            logger.info(f"[Manager] Watcher for job {job_id} is already running.")
            return

        #Create a new asyncio task for wathcing the job at background this helps manager to receive more requests
        task = asyncio.create_task(self.watch_job(job_id))
        
        #store task for future control or cancel
        self.watchers[job_id] = task
        logger.info(f"[Manager] Started background watcher for job {job_id}.")

    def stop_watcher(self, job_id):
        #search  the task and remove from dictionary
        task = self.watchers.pop(job_id, None)
        
        if task:
            #cancel task
            task.cancel()
            logger.info(f"[Manager] Stopped and removed watcher for job {job_id}.")
        else:
            logger.warning(f"[Manager] No active watcher found for job {job_id} to stop.")
