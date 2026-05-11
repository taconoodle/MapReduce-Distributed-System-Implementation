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

@pytest.fixture
def mock_s3():
    return MagicMock()

class TestMapWorker:
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

    def test_run_success(self, map_worker, mock_s3):
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

class TestShuffleWorker:
    @pytest.fixture
    def shuffle_worker(self, mock_s3):
        with patch('Jobs.worker.S3Storage', return_value=mock_s3):
            yield ShuffleWorker()

    def test_run_success(self, mock_s3, shuffle_worker):
        common_key_prefix = f'jobs/{shuffle_worker.job_id}/intermediate_files'
        input_data_prefix = common_key_prefix + f'/mapper_outputs/shuffler_{shuffle_worker.worker_id}'
        temp_output_prefix = common_key_prefix + f'/shuffler_outputs/temp/shuffler_{shuffle_worker.worker_id}'
        output_key = common_key_prefix + f'/shuffler_outputs/reducer_{shuffle_worker.worker_id}/shuffler_{shuffle_worker.worker_id}.json'
        mock_input_keys = [
            f'{input_data_prefix}/mapper_1.json',
            f'{input_data_prefix}/mapper_2.json',
        ]
        mock_sorted_keys = [
            f'{temp_output_prefix}/mapper_1.json',
            f'{temp_output_prefix}/mapper_2.json',
        ]

        mock_s3.stream_keys_in_dir.side_effect = [
            iter(mock_input_keys),
            iter(mock_sorted_keys)
        ]
        mock_s3.stream_file_pairs.return_value = iter([])

        with patch('builtins.exit') as mock_exit:
            shuffle_worker.run()

        mock_s3.init_s3.assert_called_once()
        mock_s3.sort_json.assert_has_calls([
            call('rustfs', mock_input_keys[0], 'rustfs', mock_sorted_keys[0]),
            call('rustfs', mock_input_keys[1], 'rustfs', mock_sorted_keys[1])
        ])
        mock_s3.upload_sorted_data_in_chunks.assert_called_once_with(
            'rustfs',
            output_key,
            ANY
        )
        mock_s3.close.assert_called_once()
        mock_exit.assert_called_once_with(0)

class TestReduceWorker:
    @pytest.fixture
    def reduce_worker(self, mock_s3):
        with patch('Jobs.worker.S3Storage', return_value=mock_s3):
            yield ReduceWorker()

    def test_load_reduce_fn_success(self, mock_s3, reduce_worker):
        sample_data = [
            ('a', [1, 2, 3]),
            ('b', [1, 1, 1]),
            ('c', [1, 2]),
        ]
        serialized_reduce = base64.b64encode(cloudpickle.dumps(reduce_worker.reduce_fn)).decode()
        with patch.dict(os.environ, {"SERIALIZED_REDUCE": serialized_reduce}):
            reduce_fn = reduce_worker.load_reduce_fn()
            result = list(reduce_fn(key, values) for key, values in sample_data)

        assert result == [
            ('a', 6),
            ('b', 3),
            ('c', 3)
        ]

    def test_reduce_lines_success(self, mock_s3, reduce_worker):
        sample_data = [
            ('a', 1), ('a', 2), ('a', 3),
            ('b', 1), ('b', [1, 1]),
            ('c', 1), ('c', 2)
        ]
        result = list(reduce_worker.reduce_lines(sample_data))

        assert result == [
            ('a', 6),
            ('b', 3),
            ('c', 3)
        ]

    def test_run_success(self, mock_s3, reduce_worker):
        common_key_prefix = f'jobs/{reduce_worker.job_id}'
        input_data_prefix = common_key_prefix + f'/intermediate_files/shuffler_outputs/reducer_{reduce_worker.worker_id}/shuffler_{reduce_worker.worker_id}.json'
        output_prefix = common_key_prefix + f'/output_files/job_{reduce_worker.job_id}.json'
        mock_pairs = iter([('a', 1), ('a', 2), ('b', 1)])
        mock_reduced_lines = iter([('a', 1), ('b', 1)])

        mock_s3.stream_file_pairs.return_value = mock_pairs
        with patch.object(reduce_worker, 'reduce_lines', return_value=mock_reduced_lines) as mock_reduce_lines, \
                patch('builtins.exit') as mock_exit:
            reduce_worker.run()

        mock_s3.init_s3.assert_called_once()
        mock_s3.stream_file_pairs.assert_called_once_with(
            'rustfs',
            input_data_prefix
        )
        mock_reduce_lines.assert_called_once_with(mock_pairs)
        mock_s3.upload_sorted_data_in_chunks.assert_called_once_with(
            'rustfs',
            output_prefix,
            mock_reduced_lines
        )
        mock_s3.close.assert_called_once()
        mock_exit.assert_called_once_with(0)