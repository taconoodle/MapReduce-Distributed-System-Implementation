import asyncio
import logging
import os
import base64
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import FastAPI
from kubernetes import client, config, watch
from pydantic import BaseModel
from starlette import status


# from filesplit.split import Split

from src.database import Database
from src.storage import S3Storage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


REDUCER_COUNT = 4
DATA_BUCKET = 'rustfs'
CHUNK_MAX_SIZE = 64 * 1024 * 1024

# TODO: Add 'pending upload' status
# Enums
class JobStatus(str, Enum):
    PENDING   = "Pending upload"
    QUEUED    = "Queued"
    MAP       = "Map"
    SHUFFLE   = "Shuffle"
    REDUCE    = "Reduce"
    SUCCEEDED = "Succeed"
    FAILED    = "Failed"

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
        print("IF YOU'RE READING THIS THE SERVER IS UP. LET'S FUCKING GO")

        await self.init_services()

        await self.recover_from_crash()

        await self.restart_active_watchers()

        logger.info(f"[Manager {self.replica_id}] Startup complete and listening.")

    async def handle_task_succeeded(self, job_id: str, task_id):
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

    async def transition_to_phase(self, job_id: str, new_phase: TaskType):
        self.delete_all_workers(job_id)
        self.db.update_job_status(job_id, new_phase.value)
        self.db.reset_phase_counter(job_id)
        for task_idx in range(3): # TODO: 3 is a placeholder for the amount of shuffler/maangers
            self.db.insert_task(job_id, task_idx, new_phase.value)
            if new_phase == TaskType.REDUCE:
                reduce_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/reduce_fn.pkl')['Body'].read()
                reduce_fn = base64.b64encode(reduce_fn_bytes).decode('utf-8')
                self.spawn_worker(
                    job_id=job_id,
                    task_id=task_idx,
                    phase=new_phase,
                    image="python", # TODO: Python is a placeholder
                    fn_to_pass=reduce_fn
                )
            else:
                self.spawn_worker(
                    job_id=job_id,
                    task_id=task_idx,
                    phase=new_phase,
                    image="python" # TODO: Python is a placeholder
                )

    # async def iter_lines(self, request):
    #     byte_buffer = bytearray()
    #     async for byte_chunk in request.stream():
    #         byte_buffer.extend(byte_chunk)
    #         while b'\n' in byte_buffer:
    #             newline_pos = byte_buffer.find(b'\n')
    #             line = byte_buffer[:newline_pos]
    #             del byte_buffer[:newline_pos + 1]
    #             yield line
    #     if byte_buffer:
    #         yield bytes(byte_buffer)

    # async def split_input_file(self, request, input_file: str, job_id: str):
    #     chunk_key_prefix = f'jobs/{job_id}/intermediate_files/chunks/' + input_file.name # TODO: Not sure if input file name is correct
    #
    #     num_of_chunks = 0
    #     buffer = bytearray()
    #     async for line in self.iter_lines(request):
    #         buffer.extend(line + b'\n')
    #
    #         if len(buffer) > CHUNK_MAX_SIZE:
    #             chunk_key = chunk_key_prefix + num_of_chunks
    #             self.sfs.create_object(DATA_BUCKET, chunk_key, bytes(buffer))
    #             num_of_chunks += 1
    #             buffer.clear()
    #
    #     if buffer:
    #         chunk_key = chunk_key_prefix + num_of_chunks
    #         self.sfs.create_object(DATA_BUCKET, chunk_key, bytes(buffer))
    #         num_of_chunks += 1
    #
    #     return num_of_chunks

    async def create_job(self, user_id, file_name):
        job_id = self.db.insert_job(user_id, file_name)
        return job_id

    async def init_job(self, job_id):
        input_filename = self.db.get_job_info(job_id, 'input_file_name')
        reducer_amt = 3 # TODO: Calculate this somehow
        num_of_chunks = await self.split_input_file(job_id) # TODO: FIX THIS. IT'S DEPRECATED

        map_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/map_fn.pkl')['Body'].read()
        map_fn = base64.b64encode(map_fn_bytes).decode('utf-8')

        for task_idx in range(num_of_chunks):
            self.db.insert_task(job_id, task_idx, TaskType.MAP)
            # TODO: python as the image is just a placeholder
            self.spawn_worker(job_id, task_idx, TaskType.MAP,"python", reducer_amt, map_fn)
        self.db.update_job_status(job_id, JobStatus.QUEUED)

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
        final_output_path = f"jobs/{job_id}/output_files/{job_id}.json"

        output_keys = self.sfs.stream_keys_in_dir(
            bucket=DATA_BUCKET,
            prefix=f'jobs/{job_id}/intermediate_files/reducer_outputs'
        )
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

    async def get_simple_upload_presigned_url(self, job_id, key=None):
        if key is None:
            filename = self.db.get_job_info(job_id)['input_file_name']
            key = f'jobs/{job_id}/input_files/{filename}'

        url = self.sfs.gen_url_to_put_key(DATA_BUCKET, key)
        return url

    async def get_multipart_upload_presigned_url(self, job_id, num_of_parts):
        filename = self.db.get_job_info(job_id)['input_file_name']
        key = f'jobs/{job_id}/input_files/{filename}'

        upload_id, multipart_urls = self.sfs.gen_urls_for_multipart(DATA_BUCKET, key, num_of_parts)
        return upload_id, multipart_urls

    async def complete_presigned_multipart_upload(self, job_id, upload_id, parts):
        filename = self.db.get_job_info(job_id)['input_file_name']
        key = f'jobs/{job_id}/input_files/{filename}'

        self.sfs.complete_multipart_upload(DATA_BUCKET, key, upload_id, parts)


manager_instance : Manager = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager_instance

    replica_id = os.getenv("MANAGER-ID", "manager-0")
    manager_instance = Manager(replica_id)
    await manager_instance.startup()
    yield

app = FastAPI(lifespan=lifespan)

get_manager = lambda: manager_instance

class JobSubmitRequest(BaseModel):
    user_id: int
    file_name: str
    file_size: int
    part_size: int

class JobStartRequest(BaseModel):
    user_id: int

class MultipartPresignRequest(BaseModel):
    filename: str
    file_size: int
    part_size: int

class MultipartCompleteRequest(BaseModel):
    upload_id: str
    parts: list[dict[str, int | str]] # [{"part_number": int, "etag": str}]

# @app.post("/jobs/{job_id}/uploads/simple/presign", status_code=status.HTTP_200_OK)
# async def simple_presign(job_id: int):
#     url = await manager_instance.get_simple_upload_presigned_url(job_id)
#     return {
#         "url": url
#     }

# @app.post("/jobs/{job_id}/uploads/multipart/presign", status_code=status.HTTP_200_OK)
# async def multipart_presign(job_id: int, request: MultipartPresignRequest):
#     num_of_chunks = (request.file_size // request.part_size) + 1
#
#     # TODO: Also add the file name in the database
#     upload_id, part_urls = await manager_instance.get_multipart_upload_presigned_url(job_id, num_of_chunks)
#     return {
#         'upload_id': upload_id,
#         'part_urls': part_urls
#     }

@app.post('/jobs/submit', status_code=status.HTTP_201_CREATED)
async def submit_job(request: JobSubmitRequest):
    job_id = await manager_instance.init_job(request.user_id)

    map_fn_url = await manager_instance.get_simple_upload_presigned_url(job_id, key=f'jobs/{job_id}/input_files/map_fn.pkl')
    reduce_fn_url = await manager_instance.get_simple_upload_presigned_url(job_id, key=f'jobs/{job_id}/input_files/reduce_fn.pkl')

    if request.file_size < (5 * (1024 ** 2)):
        upload_url = await manager_instance.get_simple_upload_presigned_url(job_id)
        return {
            'job_id': job_id,
            'map_url': map_fn_url,
            'reduce_url': reduce_fn_url,
            'upload_type': 'simple',
            'upload_url': upload_url
        }
    else:
        num_of_chunks = (request.file_size // request.part_size) + 1
        upload_id, part_urls = await manager_instance.get_multipart_upload_presigned_url(job_id, num_of_chunks)
        return {
            'job_id': job_id,
            'map_url': map_fn_url,
            'reduce_url': reduce_fn_url,
            'upload_type': 'multipart',
            'part_urls': part_urls
        }

@app.post("/jobs/{job_id}/uploads/multipart/complete", status_code=status.HTTP_200_OK)
async def multipart_complete(job_id: int, request: MultipartCompleteRequest):
    parts = []
    for part in request.parts:
        parts.append({'ETag': part['etag'], 'PartNumber': part['part_number']})
    await manager_instance.complete_presigned_multipart_upload(job_id, request.upload_id, parts)

@app.post('/jobs/{job_id}/start', status_code=status.HTTP_202_ACCEPTED)
async def start_job(job_id, request: JobStartRequest):
    await manager_instance.init_job(job_id)