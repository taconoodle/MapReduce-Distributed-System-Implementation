import asyncio
import hashlib
import logging
import os
import base64
from contextlib import asynccontextmanager
from enum import Enum
import threading

from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from kubernetes import client, config, watch
from pydantic import BaseModel
from starlette import status
from src.database import Database
from src.storage import S3Storage

from src.database import Database
from src.storage import S3Storage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BACKOFF_LIMIT = 3
NAMESPACE     = "default"

REDUCER_COUNT = 4
DATA_BUCKET = 'rustfs'

CHUNK_MAX_SIZE = 64 * 1024 * 1024

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
        # self.watchers: dict[str, asyncio.Task] = {}

    async def init_services(self):
        self.db.init_database()
        self.sfs.init_s3()
        config.load_incluster_config() # Use this for the container image
        # config.load_kube_config()        

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

    


    async def send_cancel_signal_to_workers(self,job_id):
        batch_v1 = client.BatchV1Api()
        try:
            batch_v1.delete_collection_namespaced_job(namespace=NAMESPACE,label_selector=f"job-id={job_id}")
        except client.exceptions.ApiException as e:
            print(f"Exception when calling BatchV1Api delete_namespaced_job: {e}")
            return False
          

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
    
@app.post("/cancel-job/{job_id}")
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
