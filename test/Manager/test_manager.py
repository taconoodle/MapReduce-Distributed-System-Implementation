import asyncio
import threading
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, call
from Manager.manager import Manager, JobStatus, TaskStatus


# ================================================================
#  Factory helpers
# ================================================================

def make_k8s_job(
    job_id: str,
    task_id: str,
    task_type: str = "Map",
    worker_num: int = 1,
    succeeded: int = None,
    failed: int = None,
    completions: int = 1,
    backoff: int = 3,
) -> MagicMock:
    """Creates a mock Kubernetes Job object."""
    k8s_job = MagicMock()
    k8s_job.metadata.labels = {
        "job_id":     job_id,
        "task_id":    task_id,
        "task_type":  task_type,
        "worker_num": str(worker_num),
        "manager_id": "0",
    }
    k8s_job.spec.completions   = completions
    k8s_job.spec.backoff_limit = backoff
    k8s_job.status.succeeded   = succeeded
    k8s_job.status.failed      = failed
    return k8s_job


def make_watch_event(
    event_type: str,
    job_id: str,
    task_id: str,
    task_type: str = "Map",
    worker_num: int = 1,
    succeeded: int = None,
    failed: int = None,
    completions: int = 1,
    backoff: int = 3,
) -> dict:
    """Creates a mock Kubernetes watch event dict."""
    return {
        "type":   event_type,
        "object": make_k8s_job(
            job_id, task_id, task_type, worker_num,
            succeeded=succeeded, failed=failed,
            completions=completions, backoff=backoff,
        ),
    }


def make_db_task(
    task_id: str,
    status: TaskStatus = TaskStatus.FAILED,
    task_type: str = "Map",
    worker_num: int = 1,
) -> dict:
    return {
        "task_id":    task_id,
        "status":     status,
        "task_type":  task_type,
        "worker_num": str(worker_num),
    }


def make_db_job(job_id: str, status: JobStatus = JobStatus.MAP) -> dict:
    return {"job_id": job_id, "status": status}


# ================================================================
#  Helper: run watch_all for a short time then cancel
# ================================================================

async def run_watcher(manager, timeout: float = 0.4):
    """
    Starts watch_all() as an asyncio task (no args — uses self.replica_id),
    lets it run for `timeout` seconds, then cancels it cleanly.
    """
    task = asyncio.create_task(manager.watch_all())
    await asyncio.sleep(timeout)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ================================================================
#  Fixture
# ================================================================

class TestManagerAdvanced:

    @pytest.fixture
    def manager(self):
        m = Manager(replica_id="0")
        m.db = MagicMock()
        m.sfs = MagicMock()
        m.handle_task_succeeded = AsyncMock()
        m.handle_job_failure    = AsyncMock()
        return m

    # ============================================================
    #  watch_all tests
    # ============================================================

    # -----------------------------------------------------------
    # Success path (MODIFIED / ADDED)
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_succeeded_calls_handle_task_succeeded(
        self, mock_batch, mock_watch, manager
    ):
        """
        MODIFIED event with succeeded >= completions must call
        handle_task_succeeded(job_id, task_id, task_type, worker_num).
        No replica_id — watch_all does not forward it to the handler.
        Note: update_task_status(COMPLETED) is commented out in the code.
        """
        event = make_watch_event(
            "MODIFIED", "job-1", "task-1",
            task_type="Map", worker_num=2, succeeded=1,
        )
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value  = JobStatus.MAP
        manager.db.get_task_status.return_value = TaskStatus.FAILED

        await run_watcher(manager)

        # Signature: handle_task_succeeded(job_id, task_id, task_type, worker_num)
        manager.handle_task_succeeded.assert_called_once_with(
            "job-1", "task-1", "Map", 2
        )

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_added_event_with_success_is_processed(
        self, mock_batch, mock_watch, manager
    ):
        """ADDED event with succeeded >= completions is handled like MODIFIED."""
        event = make_watch_event("ADDED", "job-1", "task-1", succeeded=1)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value  = JobStatus.MAP
        manager.db.get_task_status.return_value = TaskStatus.FAILED

        await run_watcher(manager)

        manager.handle_task_succeeded.assert_called_once()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_succeeded_task_already_completed_skipped(
        self, mock_batch, mock_watch, manager
    ):
        """
        Task already COMPLETED in the DB must not be processed again —
        the guard `get_task_status != COMPLETED` prevents double handling.
        """
        event = make_watch_event("MODIFIED", "job-1", "task-1", succeeded=1)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value  = JobStatus.MAP
        manager.db.get_task_status.return_value = TaskStatus.COMPLETED

        await run_watcher(manager)

        manager.handle_task_succeeded.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_succeeded_zero_is_not_success(
        self, mock_batch, mock_watch, manager
    ):
        """succeeded=0 does not satisfy succeeded >= completions(1)."""
        event = make_watch_event("MODIFIED", "job-1", "task-1", succeeded=0)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        manager.handle_task_succeeded.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_handle_task_succeeded_has_no_replica_id(
        self, mock_batch, mock_watch, manager
    ):
        """
        watch_all does NOT pass replica_id to handle_task_succeeded.
        Exactly 4 positional args: (job_id, task_id, task_type, worker_num).
        """
        event = make_watch_event(
            "MODIFIED", "job-1", "task-1",
            task_type="Reduce", worker_num=3, succeeded=1,
        )
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value  = JobStatus.REDUCE
        manager.db.get_task_status.return_value = TaskStatus.FAILED

        await run_watcher(manager)

        manager.handle_task_succeeded.assert_called_once_with(
            "job-1", "task-1", "Reduce", 3
        )

    # -----------------------------------------------------------
    # Fatal failure (failed >= backoff_limit)
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_fatal_failure_calls_handle_job_failure(
        self, mock_batch, mock_watch, manager
    ):
        """
        failed >= backoff_limit:
          1. update_task_status(job_id, task_id, FAILED)
          2. handle_job_failure(job_id)  — only job_id, no task_id or replica_id
        """
        event = make_watch_event(
            "MODIFIED", "job-1", "task-fail", failed=3, backoff=3
        )
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        # update_task_status called before handle_job_failure
        manager.db.update_task_status.assert_called_with(
            "job-1", "task-fail", TaskStatus.FAILED
        )
        # Signature: handle_job_failure(job_id)
        manager.handle_job_failure.assert_called_once_with("job-1")
        manager.handle_task_succeeded.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_fatal_failure_update_before_handler(
        self, mock_batch, mock_watch, manager
    ):
        """
        Call order is critical: update_task_status must be called
        BEFORE handle_job_failure is scheduled on the event loop.
        """
        call_order = []

        manager.db.update_task_status.side_effect = (
            lambda *a: call_order.append("update_task_status")
        )

        async def recording_failure(*a):
            call_order.append("handle_job_failure")

        manager.handle_job_failure = recording_failure

        event = make_watch_event("MODIFIED", "job-1", "task-1", failed=3, backoff=3)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        assert call_order.index("update_task_status") < call_order.index("handle_job_failure"), (
            "update_task_status must be called BEFORE handle_job_failure"
        )

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_failed_exceeds_backoff_exactly(
        self, mock_batch, mock_watch, manager
    ):
        """failed == backoff_limit is treated as permanent failure (>=)."""
        event = make_watch_event("MODIFIED", "job-1", "task-1", failed=5, backoff=5)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        manager.handle_job_failure.assert_called_once_with("job-1")

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_fatal_failure_none_backoff_defaults_to_3(
        self, mock_batch, mock_watch, manager
    ):
        """backoff_limit=None in K8s spec → default 3. failed=3 → fatal."""
        event = make_watch_event("MODIFIED", "job-1", "task-1", failed=3)
        event["object"].spec.backoff_limit = None
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        manager.handle_job_failure.assert_called_once_with("job-1")

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_success_after_fatal_failure_ignored(
        self, mock_batch, mock_watch, manager
    ):
        """
        A late success event for a job already FAILED in the DB
        must be ignored (get_job_status == FAILED guard).
        """
        events = [
            make_watch_event("MODIFIED", "job-1", "task-bad",  failed=3, backoff=3),
            make_watch_event("MODIFIED", "job-1", "task-late", succeeded=1),
        ]
        mock_watch.return_value.stream.return_value = iter(events)
        # First call → MAP, second call → FAILED (job is now dead)
        manager.db.get_job_status.side_effect = [JobStatus.MAP, JobStatus.FAILED]

        await run_watcher(manager)

        assert manager.handle_job_failure.call_count    == 1
        assert manager.handle_task_succeeded.call_count == 0

    # -----------------------------------------------------------
    # Retry within backoff limit
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_retry_within_backoff_updates_db_only(
        self, mock_batch, mock_watch, manager
    ):
        """
        failed > 0 but < backoff_limit → only update_task_status(FAILED),
        no handler called.
        """
        event = make_watch_event("MODIFIED", "job-1", "task-1", failed=1, backoff=3)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        manager.db.update_task_status.assert_called_once_with(
            "job-1", "task-1", TaskStatus.FAILED
        )
        manager.handle_job_failure.assert_not_called()
        manager.handle_task_succeeded.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_retry_at_backoff_minus_one_is_not_fatal(
        self, mock_batch, mock_watch, manager
    ):
        """failed == backoff_limit - 1 goes to retry branch, not fatal."""
        event = make_watch_event("MODIFIED", "job-1", "task-1", failed=2, backoff=3)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        manager.handle_job_failure.assert_not_called()
        manager.db.update_task_status.assert_called_once_with(
            "job-1", "task-1", TaskStatus.FAILED
        )

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_failed_zero_is_not_retry(
        self, mock_batch, mock_watch, manager
    ):
        """failed=0 does not satisfy failed > 0 — no DB update or handler."""
        event = make_watch_event("MODIFIED", "job-1", "task-1", failed=0)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        manager.db.update_task_status.assert_not_called()
        manager.handle_job_failure.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_multiple_retries_each_updates_db(
        self, mock_batch, mock_watch, manager
    ):
        """Two retry events → two separate update_task_status calls."""
        events = [
            make_watch_event("MODIFIED", "job-1", "task-1", failed=1, backoff=3),
            make_watch_event("MODIFIED", "job-1", "task-1", failed=2, backoff=3),
        ]
        mock_watch.return_value.stream.return_value = iter(events)
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        assert manager.db.update_task_status.call_count == 2

    # -----------------------------------------------------------
    # DELETED events
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_deleted_incomplete_marks_failed_and_calls_handler(
        self, mock_batch, mock_watch, manager
    ):
        """
        DELETED event for a non-COMPLETED task:
          1. update_task_status(job_id, task_id, FAILED)
          2. handle_job_failure(job_id)
        handle_task_succeeded must never be called.
        """
        event = make_watch_event("DELETED", "job-1", "task-1")
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value  = JobStatus.MAP
        manager.db.get_task_status.return_value = TaskStatus.FAILED

        await run_watcher(manager)

        manager.db.update_task_status.assert_called_once_with(
            "job-1", "task-1", TaskStatus.FAILED
        )
        # Signature: handle_job_failure(job_id)
        manager.handle_job_failure.assert_called_once_with("job-1")
        manager.handle_task_succeeded.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_deleted_completed_task_fully_ignored(
        self, mock_batch, mock_watch, manager
    ):
        """
        DELETED event for an already COMPLETED task → no update, no handler.
        The deletion is expected and nothing needs to happen.
        """
        event = make_watch_event("DELETED", "job-1", "task-1")
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value  = JobStatus.MAP
        manager.db.get_task_status.return_value = TaskStatus.COMPLETED

        await run_watcher(manager)

        manager.db.update_task_status.assert_not_called()
        manager.handle_job_failure.assert_not_called()
        manager.handle_task_succeeded.assert_not_called()

    # -----------------------------------------------------------
    # Skipping events for already-FAILED jobs
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_events_for_failed_job_fully_ignored(
        self, mock_batch, mock_watch, manager
    ):
        """
        All events for a job already FAILED in the DB must be skipped
        before any other logic runs.
        """
        events = [
            make_watch_event("MODIFIED", "job-dead", "task-1", succeeded=1),
            make_watch_event("MODIFIED", "job-dead", "task-2", failed=3, backoff=3),
        ]
        mock_watch.return_value.stream.return_value = iter(events)
        manager.db.get_job_status.return_value = JobStatus.FAILED

        await run_watcher(manager)

        manager.handle_task_succeeded.assert_not_called()
        manager.handle_job_failure.assert_not_called()
        manager.db.update_task_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_mixed_jobs_only_alive_processed(
        self, mock_batch, mock_watch, manager
    ):
        """From mixed events, only non-FAILED jobs are processed."""
        events = [
            make_watch_event("MODIFIED", "job-alive", "task-1", succeeded=1),
            make_watch_event("MODIFIED", "job-dead",  "task-2", succeeded=1),
        ]
        mock_watch.return_value.stream.return_value = iter(events)

        def job_status(job_id):
            return JobStatus.FAILED if job_id == "job-dead" else JobStatus.MAP

        manager.db.get_job_status.side_effect   = job_status
        manager.db.get_task_status.return_value = TaskStatus.FAILED

        await run_watcher(manager)

        assert manager.handle_task_succeeded.call_count == 1
        args = manager.handle_task_succeeded.call_args[0]
        assert args[0] == "job-alive"
        assert len(args) == 4  # (job_id, task_id, task_type, worker_num) — no replica_id

    # -----------------------------------------------------------
    # Unknown event type
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_unknown_event_type_is_ignored(
        self, mock_batch, mock_watch, manager
    ):
        """Events with type outside ADDED/MODIFIED/DELETED are fully ignored."""
        event = make_watch_event("BOOKMARK", "job-1", "task-1", succeeded=1)
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value = JobStatus.MAP

        await run_watcher(manager)

        manager.handle_task_succeeded.assert_not_called()
        manager.handle_job_failure.assert_not_called()
        manager.db.update_task_status.assert_not_called()

    # -----------------------------------------------------------
    # Default values (None spec fields)
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_none_completions_defaults_to_1(
        self, mock_batch, mock_watch, manager
    ):
        """completions=None in spec → default 1. succeeded=1 → success."""
        event = make_watch_event("MODIFIED", "job-1", "task-1", succeeded=1)
        event["object"].spec.completions = None
        mock_watch.return_value.stream.return_value = iter([event])
        manager.db.get_job_status.return_value  = JobStatus.MAP
        manager.db.get_task_status.return_value = TaskStatus.FAILED

        await run_watcher(manager)

        manager.handle_task_succeeded.assert_called_once()

    # -----------------------------------------------------------
    # Label selector uses self.replica_id
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_label_selector_uses_self_replica_id(
        self, mock_batch, mock_watch, manager
    ):
        """
        watch_all uses self.replica_id (not a parameter) for the K8s
        label selector: manager_id=<self.replica_id>.
        """
        mock_watch.return_value.stream.return_value = iter([])

        await run_watcher(manager)

        stream_kwargs = mock_watch.return_value.stream.call_args[1]
        assert f"manager_id={manager.replica_id}" in stream_kwargs.get("label_selector", ""), (
            f"label_selector must contain 'manager_id={manager.replica_id}'"
        )

    # -----------------------------------------------------------
    # Cancellation
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.watch.Watch")
    @patch("Manager.manager.client.BatchV1Api")
    async def test_watch_cancels_cleanly_without_exception(
        self, mock_batch, mock_watch, manager
    ):
        """Cancelling the watcher task must not raise an unexpected exception."""
        mock_watch.return_value.stream.return_value = iter([])

        task = asyncio.create_task(manager.watch_all())
        await asyncio.sleep(0.1)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass  # Expected
        except Exception as e:
            pytest.fail(f"Unexpected exception on cancellation: {e}")

    # ============================================================
    #  recover_from_crash tests
    # ============================================================

    # -----------------------------------------------------------
    # Early return when no active jobs
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_no_active_jobs_returns_early(
        self, mock_batch, manager
    ):
        """
        If get_active_jobs returns an empty list, the method must return
        immediately without touching the K8s API.
        """
        manager.db.get_active_jobs.return_value = []

        await manager.recover_from_crash()

        mock_batch.assert_not_called()
        manager.handle_job_failure.assert_not_called()
        manager.handle_task_succeeded.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_queries_correct_active_statuses(
        self, mock_batch, manager
    ):
        """get_active_jobs must be called with MAP, SHUFFLE and REDUCE."""
        manager.db.get_active_jobs.return_value = []

        await manager.recover_from_crash()

        call_args = manager.db.get_active_jobs.call_args[0][0]
        assert JobStatus.MAP     in call_args
        assert JobStatus.SHUFFLE in call_args
        assert JobStatus.REDUCE  in call_args

    # -----------------------------------------------------------
    # Fatal failure in K8s
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_fatal_failure_calls_handle_job_failure(
        self, mock_batch, manager
    ):
        """
        K8s job with failed >= backoff_limit:
          1. update_task_status(job_id, task_id, FAILED)
          2. handle_job_failure(job_id)  — only job_id, no replica_id
          3. break — no further K8s jobs for this job are processed
        """
        job_id, task_id = "job-1", "task-failed"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []

        k8s_job = make_k8s_job(job_id, task_id, failed=3, backoff=3)
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.db.update_task_status.assert_called_once_with(
            job_id, task_id, TaskStatus.FAILED
        )
        # Signature: handle_job_failure(job_id)
        manager.handle_job_failure.assert_called_once_with(job_id)
        manager.handle_task_succeeded.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_fatal_failure_exactly_at_backoff(
        self, mock_batch, manager
    ):
        """failed == backoff_limit is sufficient (>=) for permanent failure."""
        job_id, task_id = "job-1", "task-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []

        k8s_job = make_k8s_job(job_id, task_id, failed=5, backoff=5)
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.handle_job_failure.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_fatal_failure_breaks_after_first(
        self, mock_batch, manager
    ):
        """
        After the first fatal failure the loop breaks — a second failed
        task must NOT trigger another handle_job_failure call.
        """
        job_id = "job-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []

        k8s_job_a = make_k8s_job(job_id, "task-a", failed=3, backoff=3)
        k8s_job_b = make_k8s_job(job_id, "task-b", failed=3, backoff=3)
        mock_batch.return_value.list_namespaced_job.return_value.items = [
            k8s_job_a, k8s_job_b,
        ]

        await manager.recover_from_crash()

        # break stops processing after the first failure
        manager.handle_job_failure.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_none_backoff_defaults_to_3(
        self, mock_batch, manager
    ):
        """backoff_limit=None in spec → default 3. failed=3 → fatal."""
        job_id, task_id = "job-1", "task-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []

        k8s_job = make_k8s_job(job_id, task_id, failed=3)
        k8s_job.spec.backoff_limit = None
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.handle_job_failure.assert_called_once_with(job_id)

    # -----------------------------------------------------------
    # Succeeded task not yet COMPLETED in DB
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_succeeded_not_in_db_calls_handle_task_succeeded(
        self, mock_batch, manager
    ):
        """
        succeeded >= completions AND get_task_status != COMPLETED:
          - update_task_status(COMPLETED) is commented out → not called
          - handle_task_succeeded(job_id, task_id, task_type, worker_num)
            No replica_id — recover_from_crash does not pass it.
        """
        job_id, task_id = "job-1", "task-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []
        manager.db.get_task_status.return_value   = TaskStatus.FAILED

        k8s_job = make_k8s_job(
            job_id, task_id, task_type="Reduce",
            worker_num=2, succeeded=1, completions=1,
        )
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        # update_task_status(COMPLETED) is commented out → must not be called
        manager.db.update_task_status.assert_not_called()
        # Signature: handle_task_succeeded(job_id, task_id, task_type, worker_num)
        manager.handle_task_succeeded.assert_called_once_with(
            job_id, task_id, "Reduce", 2
        )

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_succeeded_already_completed_in_db_skipped(
        self, mock_batch, manager
    ):
        """Task already COMPLETED in the DB is not processed again."""
        job_id, task_id = "job-1", "task-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []
        manager.db.get_task_status.return_value   = TaskStatus.COMPLETED

        k8s_job = make_k8s_job(job_id, task_id, succeeded=1)
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.handle_task_succeeded.assert_not_called()
        manager.db.update_task_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_none_completions_defaults_to_1(
        self, mock_batch, manager
    ):
        """completions=None → default 1. succeeded=1 → success."""
        job_id, task_id = "job-1", "task-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []
        manager.db.get_task_status.return_value   = TaskStatus.FAILED

        k8s_job = make_k8s_job(job_id, task_id, succeeded=1)
        k8s_job.spec.completions = None
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.handle_task_succeeded.assert_called_once()

    # -----------------------------------------------------------
    # Task still running → no action
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_running_task_does_nothing(
        self, mock_batch, manager
    ):
        """
        Task with failed < backoff_limit and succeeded < completions
        is considered still running — left to the watcher.
        """
        job_id, task_id = "job-1", "task-running"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []

        k8s_job = make_k8s_job(job_id, task_id, failed=1, backoff=3)
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.handle_job_failure.assert_not_called()
        manager.handle_task_succeeded.assert_not_called()
        manager.db.update_task_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_task_with_no_status_fields_does_nothing(
        self, mock_batch, manager
    ):
        """Task with succeeded=None and failed=None is treated as still running."""
        job_id, task_id = "job-1", "task-pending"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = []

        k8s_job = make_k8s_job(job_id, task_id, succeeded=None, failed=None)
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.handle_job_failure.assert_not_called()
        manager.handle_task_succeeded.assert_not_called()

    # -----------------------------------------------------------
    # Orphan tasks (in DB but missing from K8s)
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_orphan_task_marks_failed_and_calls_handler(
        self, mock_batch, manager
    ):
        """
        Task present in the DB but missing from K8s (not COMPLETED):
          1. update_task_status(job_id, task_id, FAILED)
          2. handle_job_failure(job_id)
          3. break — only the first orphan triggers the handler
        """
        job_id = "job-1"
        present_task = make_db_task("task-present", status=TaskStatus.FAILED)
        orphan_task  = make_db_task("task-orphan",  status=TaskStatus.FAILED)

        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = [present_task, orphan_task]

        # Only "task-present" has a running K8s job
        k8s_job = make_k8s_job(job_id, "task-present", failed=1)
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        manager.db.update_task_status.assert_called_with(
            job_id, "task-orphan", TaskStatus.FAILED
        )
        # Signature: handle_job_failure(job_id)  — break after first orphan
        manager.handle_job_failure.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_multiple_orphans_only_first_triggers_handler(
        self, mock_batch, manager
    ):
        """
        With two orphan tasks, only the first one triggers handle_job_failure.
        The break ensures exactly one call.
        """
        job_id = "job-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = [
            make_db_task("orphan-a", status=TaskStatus.FAILED),
            make_db_task("orphan-b", status=TaskStatus.FAILED),
        ]
        mock_batch.return_value.list_namespaced_job.return_value.items = []

        await manager.recover_from_crash()

        # break after first → exactly one call
        manager.handle_job_failure.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_completed_orphan_task_not_rescheduled(
        self, mock_batch, manager
    ):
        """Orphan task that is COMPLETED in the DB must not be rescheduled."""
        job_id = "job-1"
        completed_orphan = make_db_task("task-done", status=TaskStatus.COMPLETED)

        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = [completed_orphan]

        k8s_job = make_k8s_job(job_id, "task-other", failed=1)
        mock_batch.return_value.list_namespaced_job.return_value.items = [k8s_job]

        await manager.recover_from_crash()

        for c in manager.db.update_task_status.call_args_list:
            assert c[0][1] != "task-done", "COMPLETED task-done must not be rescheduled"

    # -----------------------------------------------------------
    # No K8s jobs → fail fast on the first non-COMPLETED task
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_no_k8s_jobs_marks_first_task_failed_and_breaks(
        self, mock_batch, manager
    ):
        """
        If no K8s jobs exist for a job:
          - Find the first non-COMPLETED task
          - update_task_status(FAILED)
          - handle_job_failure(job_id)
          - break — remaining tasks are NOT touched
        """
        job_id = "job-1"
        tasks = [
            make_db_task("task-a", status=TaskStatus.FAILED),
            make_db_task("task-b", status=TaskStatus.FAILED),  # skipped by break
            make_db_task("task-c", status=TaskStatus.COMPLETED),
        ]
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = tasks
        mock_batch.return_value.list_namespaced_job.return_value.items = []

        await manager.recover_from_crash()

        # Only the first non-COMPLETED task is updated
        manager.db.update_task_status.assert_called_once_with(
            job_id, "task-a", TaskStatus.FAILED
        )
        manager.handle_job_failure.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_no_k8s_jobs_all_completed_calls_no_handler(
        self, mock_batch, manager
    ):
        """
        If no K8s jobs exist but all tasks are already COMPLETED,
        no handler or DB update is triggered.
        """
        job_id = "job-1"
        manager.db.get_active_jobs.return_value   = [make_db_job(job_id)]
        manager.db.get_tasks_for_job.return_value = [
            make_db_task("task-a", status=TaskStatus.COMPLETED),
            make_db_task("task-b", status=TaskStatus.COMPLETED),
        ]
        mock_batch.return_value.list_namespaced_job.return_value.items = []

        await manager.recover_from_crash()

        manager.handle_job_failure.assert_not_called()
        manager.handle_task_succeeded.assert_not_called()
        manager.db.update_task_status.assert_not_called()

    # -----------------------------------------------------------
    # Multiple jobs processed independently
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_multiple_jobs_processed_independently(
        self, mock_batch, manager
    ):
        """
        Each active job is processed independently:
          job-A → fatal failure → handle_job_failure(job_id)
          job-B → success      → handle_task_succeeded(job_id, task_id, task_type, worker_num)
        """
        manager.db.get_active_jobs.return_value   = [
            make_db_job("job-A"), make_db_job("job-B"),
        ]
        manager.db.get_tasks_for_job.return_value = []
        manager.db.get_task_status.return_value   = TaskStatus.FAILED

        k8s_job_a = make_k8s_job("job-A", "task-a", failed=3, backoff=3)
        k8s_job_b = make_k8s_job("job-B", "task-b", task_type="Map",
                                  worker_num=1, succeeded=1)

        def list_jobs(namespace, label_selector):
            result = MagicMock()
            result.items = [k8s_job_a] if "job-A" in label_selector else [k8s_job_b]
            return result

        mock_batch.return_value.list_namespaced_job.side_effect = list_jobs

        await manager.recover_from_crash()

        # job-A: handle_job_failure(job_id)
        manager.handle_job_failure.assert_called_once_with("job-A")
        # job-B: handle_task_succeeded(job_id, task_id, task_type, worker_num)
        manager.handle_task_succeeded.assert_called_once_with(
            "job-B", "task-b", "Map", 1
        )

    # -----------------------------------------------------------
    # Exception isolation per job
    # -----------------------------------------------------------

    @pytest.mark.asyncio
    @patch("Manager.manager.client.BatchV1Api")
    async def test_recover_exception_in_one_job_does_not_stop_others(
        self, mock_batch, manager
    ):
        """
        An exception while processing one job must be caught and logged,
        and the remaining jobs must continue to be processed.
        """
        manager.db.get_active_jobs.return_value   = [
            make_db_job("job-boom"), make_db_job("job-ok"),
        ]
        manager.db.get_tasks_for_job.return_value = []
        manager.db.get_task_status.return_value   = TaskStatus.FAILED

        k8s_job_ok = make_k8s_job("job-ok", "task-ok", succeeded=1)

        def list_jobs(namespace, label_selector):
            if "job-boom" in label_selector:
                raise RuntimeError("K8s API unavailable")
            result = MagicMock()
            result.items = [k8s_job_ok]
            return result

        mock_batch.return_value.list_namespaced_job.side_effect = list_jobs

        # Must not raise
        await manager.recover_from_crash()

        # job-ok was still processed correctly
        manager.handle_task_succeeded.assert_called_once_with(
            "job-ok", "task-ok", "Map", 1
        )