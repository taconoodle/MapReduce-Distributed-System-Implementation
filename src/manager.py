import asyncio
import logging
import os
import base64
from contextlib import asynccontextmanager
from enum import StrEnum

from fastapi import FastAPI, HTTPException, Depends
from kubernetes_asyncio import client, watch, config
from kubernetes_asyncio.client import ApiClient
from pydantic import BaseModel
from starlette import status

from src.database import Database
from src.storage import S3Storage


logging.basicConfig(
    filename='/app/logs/manager.log',
    level=logging.INFO,
    format='{asctime} - {levelname} - {module}:{funcName} -- {msg}',
    style='{',
    datefmt='%d/%m/%Y %H:%M:%S'
)
logger = logging.getLogger(__name__)


BACKOFF_LIMIT = 3
NAMESPACE     = "default"

REDUCER_COUNT = 4
DATA_BUCKET = 'data-bucket'

CHUNK_MAX_SIZE = 64 * 1024 * 1024

# Enums
class JobStatus(StrEnum):
    PENDING   = "Pending upload"
    MAP       = "Map"
    SHUFFLE   = "Shuffle"
    REDUCE    = "Reduce"
    SUCCEEDED = "Succeed"
    FAILED    = "Failed"

class TaskType(StrEnum):
    MAP     = "Map"
    SHUFFLE = "Shuffle"
    REDUCE  = "Reduce"

class TaskStatus(StrEnum):
    PENDING   = "Pending"
    COMPLETED = "Completed"

class Manager:
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.db  = Database()
        self.sfs = S3Storage()
        self.max_containers_per_node = 50 # TODO: Get this somehow
        self.number_of_nodes = 1 # TODO: Get this somehow

    async def init_services(self):
        self.db.init_database()
        self.sfs.init_s3(DATA_BUCKET)
        config.load_incluster_config() # Use this for the container image
        # config.load_kube_config()

    async def startup(self):
        logger.info(f"Starting up...")

        await self.init_services()

        await self.recover_from_crash()

        watcher_task = asyncio.create_task(self.watch_all())

        logger.info(f"Startup complete and listening.")
        return watcher_task

    async def handle_k8s_job(self, k8s_job, job_id=None, job_chunk_num=None, job_reducer_num=None):
        k8s_job_conditions = k8s_job.status.conditions or []
        terminal_condition = next(
            (condition for condition in k8s_job_conditions if
             condition.status == "True" and condition.type in ["Complete", "Failed"]),
            None
        )
        # We only want to handle jobs that have reached a terminal status
        if not terminal_condition:
            return

        # Inspect each K8s job and decide what action to take
        # based on its current status.
        job_id = k8s_job.metadata.labels.get('job_id') if job_id is None else job_id
        job_chunk_num = self.db.get_job_info(job_id, 'total_chunks') if job_chunk_num is None else job_chunk_num # TODO: One of these calls is redundant
        job_reducer_num = self.db.get_job_info(job_id, 'reducer_amount') if job_reducer_num is None else job_reducer_num

        task_id = k8s_job.metadata.labels.get("task_id")
        task_type = k8s_job.metadata.labels.get("task_type")
        total_workers = job_chunk_num if task_type == TaskType.MAP else job_reducer_num

        if terminal_condition.type == "Complete":
            # logger.info(f"Job: {job_id} -- Task: {task_id} -- {task_type}: SUCCEEDED.")
            await self.handle_task_succeeded(job_id, task_id, task_type,
                                             total_workers)  # TODO: Check if we need to check if the DB task is already marked as completed
        elif terminal_condition.type == "Failed":
            # The task has exhausted all retries — permanent failure.
            # Update the database and invoke the failure handler,
            # which will stop the entire job.
            logger.warning(f"Job: {job_id} -- Task: {task_id} -- {task_type}: FAILED. Terminating Job.")
            # self.db.update_job_status(job_id, JobStatus.FAILED)
            await self.cleanup_job(job_id, JobStatus.FAILED)
            return 'Job Failed'

    async def restart_orphaned_tasks(self, job_id, active_k8s_jobs=None):
        # Detect orphaned tasks: present in the database but missing a
        # corresponding K8s job — likely lost during the crash.

        if active_k8s_jobs is None:
            selector = f"job_id={job_id},manager_id={self.replica_id}"
            active_k8s_jobs = client.BatchV1Api().list_namespaced_job(namespace=NAMESPACE, label_selector=selector)

        # Tasks for this job as known by the database.
        tasks_in_db = self.db.get_tasks_for_job(job_id)

        # Set of task_ids that have a corresponding K8s job
        active_k8s_task_ids = {
            k8s_job.metadata.labels.get("task_id") for k8s_job in active_k8s_jobs.items
        }

        job_status = self.db.get_job_status(job_id)
        map_fn = None
        reduce_fn = None
        for task in tasks_in_db:
            if task['task_id'] not in active_k8s_task_ids and task['status'] != TaskStatus.COMPLETED:
                logger.warning(f"Job: {job_id} -- Task: {task['task_id']} -- {task['task_type']} is missing from Kubernetes. Rescheduling...")
                # TODO: We have to restart the K8s jobs
                # We declare beforehand the arguments we will use to respawn the worker as a dict
                # The dict will get unpacked on call
                spawn_worker_args = {
                    'job_id': job_id,
                    'task_id': task['task_id'],
                    'phase': job_status,
                    'image': 'python:3.14-slim'
                }

                # This block is a bit dumbly implemented TBH.
                # Above, I have declared the map/reduce function variables as None, since they may be used or the may be not.
                # If they are used they are set correctly the first time.
                # To avoid unnecessary requests, check if the map/reduce function variables are set.
                # If they have one, we don't need to ask the SFS for it again, since the value stays the same for the same job.
                if job_status == JobStatus.MAP:
                    if not map_fn:
                        map_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/map_fn.pkl')['Body'].read()
                        map_fn = base64.b64encode(map_fn_bytes).decode('utf-8')
                    spawn_worker_args['fn_to_pass'] = map_fn
                elif job_status == JobStatus.REDUCE:
                    if not reduce_fn:
                        reduce_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/reduce_fn.pkl')['Body'].read()
                        reduce_fn = base64.b64encode(reduce_fn_bytes).decode('utf-8')
                    spawn_worker_args['fn_to_pass'] = reduce_fn

                # Unpack the args dict
                await self.spawn_worker(**spawn_worker_args)


    async def recover_from_crash(self):
        # Statuses considered "active" — jobs in these stages

        active_statuses = [JobStatus.MAP, JobStatus.SHUFFLE, JobStatus.REDUCE]
        active_jobs = self.db.get_active_jobs(active_statuses)

        # If no active jobs exist, no action is needed.
        if not active_jobs:
            logger.info(f"No active jobs found in DB to recover.")
            return

        async with ApiClient() as api:
            batch = client.BatchV1Api(api)

            for job_id in active_jobs:
                try:
                    job_phase_before_recovery = self.db.get_job_status(job_id)
                    # First, synchronize the database on how many tasks are completed
                    self.db.count_and_set_counter(job_id)

                    # Fetch only the K8s jobs that belong to this job
                    # and are managed by this Manager replica.
                    selector = f"job_id={job_id},manager_id={self.replica_id}"
                    k8s_jobs = await batch.list_namespaced_job(namespace=NAMESPACE, label_selector=selector)

                    job_chunks = self.db.get_job_info(job_id, 'total_chunks')
                    job_reducer_num = self.db.get_job_info(job_id, 'reducer_amount')

                    for k8s_job in k8s_jobs.items:
                        task_id = k8s_job.metadata.labels.get("task_id")
                        task_type = k8s_job.metadata.labels.get("task_type")
                        if self.db.get_task_status(job_id, task_id, task_type) == TaskStatus.COMPLETED:
                            continue

                        if await self.handle_k8s_job(k8s_job, job_id, job_chunks, job_reducer_num) == 'Job Failed':
                            break

                    # We don't need to find hanging tasks if the job has already transitioned to the next phase
                    if job_phase_before_recovery == self.db.get_job_status(job_id):
                        await self.restart_orphaned_tasks(job_id, k8s_jobs)

                except Exception as e:
                    # If recover fails for one job, log the error
                    # and continue processing the remaining jobs.
                    logger.error(f"Error recovering job {job_id}: {e}")

        logger.info(f"Recover complete.")

    async def watch_all(self):
        # Only watch K8s jobs that belong to this Manager replica.
        selector = f"manager_id={self.replica_id}"
        logger.info(f"Watcher started.")
        try:
            while True:
                try:
                    async with ApiClient() as api:
                        batch = client.BatchV1Api(api)
                        w = watch.Watch()
                        async with w.stream(
                                    batch.list_namespaced_job,
                                    namespace=NAMESPACE,
                                    label_selector=selector
                            ) as stream:
                            async for event in stream:
                                k8s_job = event["object"]
                                event_type = event["type"]

                                labels = k8s_job.metadata.labels or {}
                                job_id = labels.get("job_id")
                                task_id = labels.get("task_id")
                                task_type = labels.get("task_type")

                                task_status = self.db.get_task_status(job_id, task_id, task_type)

                                # 1. Skip all events for user jobs that have already been marked as FAILED —
                                #    no further processing is meaningful for a dead job.
                                # 2. Ignore any event types outside the three we handle.
                                # 3. Skip the handling for the K8s jobs that have been marked as completed already
                                #    no point in handling them (they'll get deleted on the phase transition)
                                if self.db.get_job_status(job_id) == JobStatus.FAILED or \
                                        event_type not in ("MODIFIED", "DELETED") or \
                                        task_status == TaskStatus.COMPLETED:
                                    continue

                                if event_type == "DELETED" and task_status == TaskStatus.PENDING:
                                    # If the K8s job was deleted before completing, reschedule
                                    # If it was already COMPLETED,
                                    # the deletion is expected and we do nothing.
                                    logger.info(f"Job: {job_id} -- Task: {task_id} -- {task_type}: K8s Job crashed. Rescheduling... In database it is: {task_status}")
                                    # TODO: Reschedule K8s job
                                    # We declare beforehand the arguments we will use to respawn the worker as a dict
                                    # The dict will get unpacked on call
                                    spawn_worker_args = {
                                        'job_id': job_id,
                                        'task_id': task_id,
                                        'phase': task_type,
                                        'image': 'python:3.14-slim'
                                    }

                                    # This block is a bit dumbly implemented TBH.
                                    # Above, I have declared the map/reduce function variables as None, since they may be used or the may be not.
                                    # If they are used they are set correctly the first time.
                                    # To avoid unnecessary requests, check if the map/reduce function variables are set.
                                    # If they have one, we don't need to ask the SFS for it again, since the value stays the same for the same job.
                                    if task_status == JobStatus.MAP:
                                        if not map_fn:
                                            map_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/map_fn.pkl')['Body'].read()
                                            map_fn = base64.b64encode(map_fn_bytes).decode('utf-8')
                                        spawn_worker_args['fn_to_pass'] = map_fn
                                    elif task_type == JobStatus.REDUCE:
                                        if not reduce_fn:
                                            reduce_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/reduce_fn.pkl')['Body'].read()
                                            reduce_fn = base64.b64encode(reduce_fn_bytes).decode('utf-8')
                                        spawn_worker_args['fn_to_pass'] = reduce_fn

                                    # Unpack the args dict
                                    await self.spawn_worker(**spawn_worker_args)

                                elif event_type == "MODIFIED":
                                    # logger.info(f'Job: {job_id} -- Task: {task_id} -- {task_type}: K8s Job modified. Handling:')
                                    await self.handle_k8s_job(k8s_job)

                except asyncio.CancelledError:
                    logger.info(f"Watcher cancelled.")
                    raise
                except Exception as e:
                    logger.error(f"Watcher error: {e}")
                    await asyncio.sleep(5)
        finally:
            logger.info(f"Watcher exited.")

    async def handle_task_succeeded(self,job_id, task_id, task_type, worker_num_for_phase_completion):
        self.db.update_task_status(job_id, task_id, TaskStatus.COMPLETED)
        # logger.info(f'Job: {job_id} -- Task: {task_id} -- {task_type}: New task status: {self.db.get_task_status(job_id, task_id, task_type)}')
        completed = self.db.increment_and_fetch_counters(job_id)

        logger.info(f"Job: {job_id} -- Task: {task_id} -- {task_type}: SUCCEEDED. ({worker_num_for_phase_completion - completed} remain for phase transition)")

        if completed == worker_num_for_phase_completion:
            if task_type == TaskType.MAP:
                # transition to shuffle
                await self.transition_to_phase(job_id, TaskType.SHUFFLE)

            elif task_type == TaskType.SHUFFLE:
                # transition to reduce
                await self.transition_to_phase(job_id, TaskType.REDUCE)

            elif task_type == TaskType.REDUCE:
                # finalize the job
                # url = await self.finalize_job(job_id) # TODO: ADD THIS BACK
                await self.cleanup_job(job_id, JobStatus.SUCCEEDED) # TODO: Remove this
                # notify UI
                # return url # TODO: ADD THIS BACK

        return None

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

    async def send_cancel_signal_to_workers(self, job_id):
        async with ApiClient() as api:
            batch_v1 = client.BatchV1Api(api)
            try:
                job_status = self.db.get_job_status(job_id)
                logger.info(f'Job: {job_id} -- {job_status} finished. Sending cancel signal to workers')
                await batch_v1.delete_collection_namespaced_job(
                    namespace=NAMESPACE,
                    label_selector=f"job_id={job_id}",
                    body=client.V1DeleteOptions(propagation_policy="Background")
                )
                self.db.delete_all_tasks(job_id)
                return True
            except client.exceptions.ApiException as e:
                print(f"Exception when calling BatchV1Api delete_namespaced_job: {e}")
                return False

    async def transition_to_phase(self, job_id: str, new_phase: TaskType):
        await self.send_cancel_signal_to_workers(job_id)
        self.db.update_job_status(job_id, new_phase)
        self.db.reset_phase_counter(job_id)

        reduce_number = self.db.get_job_info(job_id, 'reducer_amount')
        for task_idx in range(reduce_number): # TODO: 3 is a placeholder for the amount of shuffler/maangers
            self.db.insert_task(job_id, task_idx, new_phase)
            if new_phase == TaskType.REDUCE:
                reduce_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/reduce_fn.pkl')['Body'].read()
                reduce_fn = base64.b64encode(reduce_fn_bytes).decode('utf-8')
                await self.spawn_worker(
                    job_id=job_id,
                    task_id=task_idx,
                    phase=new_phase,
                    image="python:3.14-slim", # TODO: Python is a placeholder
                    fn_to_pass=reduce_fn
                )
            else:
                await self.spawn_worker(
                    job_id=job_id,
                    task_id=task_idx,
                    phase=new_phase,
                    image="python:3.14-slim" # TODO: Python is a placeholder
                )

    def get_required_reducer_amount(self):
        reducer_amount = 0.95 * self.max_containers_per_node * self.number_of_nodes
        return int(reducer_amount)

    async def create_job(self, user_id, file_name):
        job_id = self.db.insert_job(user_id, file_name)
        return job_id

    async def init_job(self, job_id):
        input_filename = self.db.get_job_info(job_id, 'input_file_name')
        reducer_amt = self.get_required_reducer_amount()
        self.db.update_job_reducer_amount(job_id, reducer_amt)

        # num_of_chunks = 3 # TODO: THAT'S A PLACEHOLDER
        num_of_chunks = self.sfs.split_key_to_chunks(
            bucket=DATA_BUCKET,
            key=f'jobs/{job_id}/input_files/{input_filename}',
            destination_prefix=f'jobs/{job_id}/intermediate_files/chunks/',
            part_size=64 * (1024 ** 2)
        )
        self.db.update_job_chunks(job_id, num_of_chunks)

        map_fn_bytes = self.sfs.get_key(DATA_BUCKET, f'jobs/{job_id}/input_files/map_fn.pkl')['Body'].read()
        map_fn = base64.b64encode(map_fn_bytes).decode('utf-8')

        for task_idx in range(num_of_chunks):
            self.db.insert_task(job_id, task_idx, TaskType.MAP)
            # TODO: python as the image is just a placeholder
            await self.spawn_worker(job_id, task_idx, TaskType.MAP,"python:3.14-slim", reducer_amt, map_fn)
        self.db.update_job_status(job_id, JobStatus.MAP)

    async def cleanup_job(self, job_id, job_status: JobStatus):
        await self.send_cancel_signal_to_workers(job_id)

        intermediate_prefix = f"jobs/{job_id}/intermediate_files/"
        try:
            # for key in self.sfs.stream_keys_in_dir(DATA_BUCKET, intermediate_prefix): # TODO: ADD THESE BACK
            #     self.sfs.delete_from_bucket(DATA_BUCKET, key)
            logger.info(f"Job: {job_id}: Intermediate storage cleared.")
        except Exception as e:
            logger.warning(f"Cleanup failed for job {job_id}: {e}")

        self.db.delete_all_tasks(job_id)
        self.db.update_job_status(job_id, job_status)

    #check this for output prefix Πήρα τις μεθόδους του storage από το δικό σου
    # Thanks bro
    async def finalize_job(self, job_id):
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
        logger.info(f"Job {job_id}: Job complete.")

        return url

    async def spawn_worker(self, job_id, task_id, phase, image, reducer_amount=None, worker_index=None, fn_to_pass=None):
        if worker_index is None:
            worker_index = task_id

        async with ApiClient() as api:
            batch_v1 = client.BatchV1Api(api)

            worker_name = f"job-{job_id}-{phase.lower()}-{task_id}"

            env_variables = [
                    client.V1EnvVar(name="JOB_ID", value=str(job_id)),
                    client.V1EnvVar(name="WORKER_IDX", value=str(worker_index))
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

            labels = {
                'manager_id': self.replica_id,
                'job_id': str(job_id),
                'task_id': str(task_id),
                'task_type': phase
            }
            template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=labels),
                spec=client.V1PodSpec(containers=[container], restart_policy="Never")
            )

            job_spec = client.V1JobSpec(
                template=template,
                backoff_limit=3,
                active_deadline_seconds=300,
                ttl_seconds_after_finished=120 # TODO: This may have to be infinite
            )

            k8s_job = client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=client.V1ObjectMeta(name=worker_name, labels=labels),
                spec=job_spec
            )

            await batch_v1.create_namespaced_job(namespace="default", body=k8s_job) # TODO: Use asyncio here

    # def delete_all_workers(self, job_id):
    #     batch_v1 = client.BatchV1Api()
    #
    #     jobs = batch_v1.list_namespaced_job(namespace="default")
    #     for job in jobs.items:
    #         if str(job_id) in job.metadata.name:
    #             try:
    #                 batch_v1.delete_namespaced_job(
    #                     name=job.metadata.name,
    #                     namespace="default",
    #                     body=client.V1DeleteOptions(propagation_policy="Foreground")
    #                 )
    #             except: pass

    def delete_worker(self, worker_name, ):
        batch_v1 = client.BatchV1Api()

        batch_v1.delete_namespaced_job(
            name=worker_name,
            namespace="default",
            propagation_policy="Background"
        )

    async def get_simple_upload_presigned_url(self, job_id, key=None):
        if key is None:
            filename = self.db.get_job_info(job_id, 'input_file_name')
            key = f'jobs/{job_id}/input_files/{filename}'

        url = self.sfs.gen_url_to_put_key(DATA_BUCKET, key)
        return url

    async def get_multipart_upload_presigned_url(self, job_id, num_of_parts):
        filename = self.db.get_job_info(job_id, 'input_file_name')
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

    watcher_task = await manager_instance.startup()
    yield
    watcher_task.cancel()
    try:
        await watcher_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

class JobSubmitRequest(BaseModel):
    user_id: str
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

# @app.post('/jobs/submit', status_code=status.HTTP_201_CREATED)
@app.post('/jobs/submit')
async def submit_job(request: JobSubmitRequest):
    job_id = await manager_instance.create_job(request.user_id, request.file_name)

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

# async def verify_token(token: str = Depends()):#inside Depends something on Keycloak
#     return

async def verify_token():#inside Depends something on Keycloak
    return

@app.post("/cancel-job/{job_id}")
async def cancel_job(job_id, token_data: dict = Depends(verify_token)):
    success = await manager_instance.send_cancel_signal_to_workers(job_id)

    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"Job {job_id} not found or already completed",)

    #Maybe this
    #tasks = self.db.get_tasks_for_job(job_id)
    #for task in tasks:
    #   task_id = task["task_id"]
    #   self.db.update_task_status(task_id,TaskStatus.FAILED)
    #asyncio.run_coroutine_threadsafe(self.handle_job_failure(job_id,self.replica_id))
    return {"status": "cancelled", "job_id": job_id}

@app.get("/")
async def root():
    return {"message": "Hello World"}