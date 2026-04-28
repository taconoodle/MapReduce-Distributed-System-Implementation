from Manager.manager import *

import pytest
from unittest.mock import patch, MagicMock, ANY

class TestManager:
    @pytest.fixture
    def manager(self):
        return Manager('0')

    def test_spawn_worker_success_without_mock_k8s(self, manager):
        job_id = 0
        task_ids = [i for i in range(0, 10)]
        phase = TaskType.MAP
        reducer_amount = 4
        image = 'python'

        for task in task_ids:
            manager.spawn_worker(job_id, task, phase, reducer_amount, image)