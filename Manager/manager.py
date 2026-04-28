import asyncio
import logging
import os
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from kubernetes import client, config, watch
# from filesplit.split import Split

from Manager.database import Database
from Manager.storage import S3Storage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


REDUCER_COUNT = 4
DATA_BUCKET = 'rustfs'
CHUNK_MAX_SIZE = 64 * 1024 * 1024

# Enums
class JobStatus(str, Enum):
    PENDING  = "Pending"
    MAP      = "Map"
    SHUFFLE  = "Shuffle"
    REDUCE   = "Reduce"
    SUCCEEDED  = "Succeed"
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
        # config.load_incluster_config() # Use this for the container image
        config.load_kube_config()

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

    async def transition_to_phase(self, job_id, new_phase: TaskType):
        self.delete_all_workers(job_id)
        self.db.update_job_status(job_id, new_phase.value)
        self.db.reset_phase_counter(job_id)
        for task_idx in range(3): # TODO: 3 is a placeholder for the amount of shuffler/maangers
            self.db.insert_task(job_id, task_idx, new_phase.value)
            self.spawn_worker(job_id=job_id,
                              task_id=task_idx,
                              phase=new_phase,
                              image="python" # TODO: Python is a placeholder
            )

    async def iter_lines(self, request):
        byte_buffer = bytearray()
        async for byte_chunk in request.stream():
            byte_buffer.extend(byte_chunk)
            while b'\n' in byte_buffer:
                newline_pos = byte_buffer.find(b'\n')
                line = byte_buffer[:newline_pos]
                del byte_buffer[:newline_pos + 1]
                yield line
        if byte_buffer:
            yield bytes(byte_buffer)

    async def split_input_file(self, request, input_file, job_id):
        chunk_key_prefix = f'jobs/{job_id}/intermediate_files/chunks/' + input_file.name # TODO: Not sure if input file name is correct

        num_of_chunks = 0
        buffer = bytearray()
        async for line in self.iter_lines(request):
            buffer.extend(line + b'\n')

            if len(buffer) > CHUNK_MAX_SIZE:
                chunk_key = chunk_key_prefix + num_of_chunks
                self.sfs.create_object(DATA_BUCKET, chunk_key, bytes(buffer))
                num_of_chunks += 1
                buffer.clear()

        if buffer:
            chunk_key = chunk_key_prefix + num_of_chunks
            self.sfs.create_object(DATA_BUCKET, chunk_key, bytes(buffer))
            num_of_chunks += 1

        return num_of_chunks

    async def init_job(self, user_id, request, input_filename, output_filename):
        reducer_amt = 3
        job_id = self.db.insert_job(user_id, input_filename, output_filename)
        num_of_chunks = await self.split_input_file(request, input_filename, job_id)

        for task_idx in range(num_of_chunks):
            self.db.insert_task(job_id, task_idx, TaskType.MAP)
            # TODO: python as the image is just a placeholder
            self.spawn_worker(job_id, task_idx, TaskType.MAP, reducer_amt, "python")

    async def cleanup_job(self, job_id, job_status: JobStatus):
        self.delete_all_workers(job_id)

        intermediate_prefix = f"jobs/{job_id}/intermediate_files/"
        try:
            for key in self.sfs.stream_keys_in_dir(DATA_BUCKET, intermediate_prefix):
                self.sfs.delete_from_bucket(DATA_BUCKET, key)
            logger.info(f"[Job {job_id}] Intermediate storage cleared.")
        except Exception as e:
            logger.warning(f"Cleanup failed for job {job_id}: {e}")

        self.db.delete_all_tasks(job_id)
        self.db.update_job_status(job_id, job_status)
        # TODO: This will most probably be removed, we'll see after Menbal implements the watchers
        self.stop_watcher(job_id)

    #check this for output prefix Πήρα τις μεθόδους του storage από το δικό σου
    # Thanks bro
    async def finalize_job(self, job_id, job_status: JobStatus):
        out_file_name = self.db.get_job_info(job_id, "output_file_name")
        final_output_path = f"jobs/{job_id}/output_files/{out_file_name}.json"

        output_keys = self.sfs.stream_keys_in_dir(DATA_BUCKET, f'jobs/{job_id}/intermediate_files/reducer_outputs')
        self.sfs.merge_keys_unsorted(
            source_bucket=DATA_BUCKET,
            source_keys=list(output_keys),
            output_key=final_output_path
        )
        url = self.sfs.gen_url_to_get_key(DATA_BUCKET, final_output_path)

        await self.cleanup_job(job_id, JobStatus.SUCCEEDED)
        logger.info(f"[Job {job_id}] Finalization complete.")

        return url

    def spawn_worker(self, job_id, task_id, phase, image, reducer_amount=None, chunk_to_process=None, fn_to_pass=None):
        if chunk_to_process is None:
            chunk_to_process = task_id
        batch_v1 = client.BatchV1Api()

        worker_name = f"job-{job_id}-{phase.value.lower()}-{task_id}"

        env_variables = [
                client.V1EnvVar(name="JOB_ID", value=str(job_id)),
                client.V1EnvVar(name="WORKER_ID", value=str(chunk_to_process)),
        ]
        match phase:
            case TaskType.MAP:
                env_variables.append(client.V1EnvVar(name="WORKER_NUM", value=str(reducer_amount)))
                env_variables.append(client.V1EnvVar(name="SERIALIZED_MAP", value=fn_to_pass))
            case TaskType.REDUCE:
                env_variables.append(client.V1EnvVar(name="SERIALIZED_REDUCE", value=fn_to_pass))
            case _:
                pass

        container = client.V1Container(
            name="worker",
            image=image, # TODO: Fill the correct image. We have to containerize the workers first
            env=env_variables
        )

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={
                "manager_id": self.replica_id,
                "job_id": str(job_id),
                "task_id": str(task_id),
                "phase": phase.value
            }),
            spec=client.V1PodSpec(containers=[container], restart_policy="Never")
        )

        job_spec = client.V1JobSpec(
            template=template,
            backoff_limit=3,
            active_deadline_seconds=300,
            ttl_seconds_after_finished=120
        )

        k8s_job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=worker_name),
            spec=job_spec
        )

        batch_v1.create_namespaced_job(namespace="default", body=k8s_job)

    def delete_all_workers(self, job_id):
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

    def delete_worker(self, worker_name, ):
        batch_v1 = client.BatchV1Api()

        batch_v1.delete_namespaced_job(
            name=worker_name,
            namespace="default",
            propagation_policy="Background"
        )

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
