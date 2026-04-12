from Jobs.worker import *

import pytest
from unittest.mock import patch, MagicMock, ANY, mock_open, call
import cloudpickle
import base64
import os

MOCK_DATA_LIST = [
            ('a', [2, 'one more']),
            ('b', 'oog'),
            ('c', {'apple': 1})
            ]

MOCK_DATA_DICT = {
            'a': [2, 'one more'],
            'b': 'oog',
            'c': {'apple': 1}
            }

class TestMapWorker:

    @pytest.fixture
    def mock_s3(self):
        return MagicMock()

    @pytest.fixture
    def map_worker(self, mock_s3):
        with patch('Jobs.worker.S3Storage', return_value=mock_s3):
            yield MapWorker()

    def test_map_fn_success(self, map_worker):
        result = list(map_worker.map_fn(MOCK_DATA_DICT))
        assert result == [
            ('a', 1),
            ('b', 1),
            ('c', 1)
        ]

    def test_load_map_fn_success(self, map_worker):
        serialized_map = base64.b64encode(cloudpickle.dumps(map_worker.map_fn)).decode()
        with patch.dict(os.environ, {"SERIALIZED_MAP": serialized_map}):
            map_fn = map_worker.load_map_fn()

        result = list(map_fn(MOCK_DATA_DICT))
        assert result == [
            ('a', 1),
            ('b', 1),
            ('c', 1)
        ]

    def test_get_chunk_success(self, map_worker, mock_s3):
        map_worker.get_chunk()
        mock_s3.download_from_bucket.assert_called_once_with(
            map_worker.data_bucket,
            f'jobs/{map_worker.job_id}/intermediate_files/chunks/chunk_{map_worker.worker_id}',
            '/data/chunk'
        )

    def test_get_responsible_shuffler_success(self, map_worker, mock_s3):
        result = map_worker.get_responsible_shuffler(4)
        assert result == 1

    def test_get_map_results_success(self, map_worker):
        result = map_worker.get_map_results(MOCK_DATA_DICT)
        for key in MOCK_DATA_DICT:
            shuffler_id = map_worker.get_responsible_shuffler(key)
            assert shuffler_id in result
            assert key in result[shuffler_id]
            assert result[shuffler_id][key] == [1]

    def test_run_calls_correct_sequence(self, map_worker, mock_s3):
        mock_data = {"a": 1, "b": 2}
        mock_map_results = {0: {"a": [1]}, 1: {"b": [2]}}
        expected_calls = [
            call('rustfs', '/data/output', f'jobs/1/intermediate_files/mapper_outputs/shuffler_0/mapper_1.json'),
            call('rustfs', '/data/output', f'jobs/1/intermediate_files/mapper_outputs/shuffler_1/mapper_1.json'),
        ]

        with patch.object(map_worker, 'get_chunk') as mock_get_chunk, \
                patch.object(map_worker, 'get_map_results', return_value=mock_map_results) as mock_get_map_results, \
                patch('builtins.open', mock_open(read_data=json.dumps(mock_data))), \
                patch('builtins.exit') as mock_exit, \
                patch('json.load', return_value=mock_data), \
                patch('json.dump'):
            map_worker.run()

        # verify correct sequence
        mock_s3.init_s3.assert_called_once()
        mock_get_chunk.assert_called_once()
        mock_get_map_results.assert_called_once_with(mock_data)
        mock_s3.upload_to_bucket.assert_has_calls(expected_calls)
        mock_s3.close.assert_called_once()
        mock_exit.assert_called_once_with(0)
