from Manager.storage import S3Storage
from collections import defaultdict
import json
import heapq
import os
import base64
import cloudpickle

class MapWorker:
    def __init__(self):
        # self.job_id = os.getenv("JOB_ID")
        # self.worker_id = os.getenv("WORKER_ID")
        # self.worker_num = os.getenv("WORKER_NUM")
        # self.map_fn = self.load_map_fn()
        self.job_id = 1
        self.worker_id = 1
        self.worker_num = 3

        self.data_bucket = 'rustfs'
        self.s3 = S3Storage()

    def map_fn(self, data):
        for k, v in data.items():
            yield k, 1

    def load_map_fn(self):
        serialized_map_fn = os.getenv("SERIALIZED_MAP")
        return cloudpickle.loads(base64.b64decode(serialized_map_fn))

    def get_chunk(self):
        chunk_key = f'jobs/{self.job_id}/intermediate_files/chunks/chunk_{self.worker_id}'
        self.s3.download_from_bucket(self.data_bucket, chunk_key, f'/data/chunk')

    def get_responsible_shuffler(self, key):
        key_hash = hash(key)
        shuffler_id = key_hash % self.worker_num
        return shuffler_id

    def get_map_results(self, data):
        # For each part of the chunk, perform the map function
        results = defaultdict(lambda: defaultdict(list))

        # TODO: Possibly obsolete
        # for item in data.items():
        #     kv_pairs = self.map_fn(item)
        #     for key, value in kv_pairs:
        #         shuffler_id = self.get_responsible_shuffler(key)
        #         results[shuffler_id][key].extend(value)

        kv_pairs = self.map_fn(data)
        for key, value in kv_pairs:
            shuffler_id = self.get_responsible_shuffler(key)
            results[shuffler_id][key].append(value)
        # Results will be of the format: {shuffler_id: {key: [values]}}
        return results
            
    def run(self):
        # TODO: Maybe there's a more efficient way to do this by streaming
        self.s3.init_s3()
        self.get_chunk()
        chunk_local_path = f'/data/chunk'

        with open(chunk_local_path, 'r') as file:
            data = json.load(file)

        map_results = self.get_map_results(data)

        # Write the dictionary entry of each shuffler to a json file
        for shuffler_id, shuffler_data in map_results.items():
            output_key = f'jobs/{self.job_id}/intermediate_files/mapper_outputs/shuffler_{shuffler_id}/mapper_{self.worker_id}.json'
            with open('/data/output', 'w') as file:
                json.dump(shuffler_data, file)
            self.s3.upload_to_bucket(self.data_bucket, '/data/output', output_key)

        self.s3.close()
        exit(0)
    
class ShuffleWorker:
    def __init__(self):
        self.job_id = None
        self.worker_id = None
        self.data_bucket = 'rustfs'
        self.s3 = S3Storage()
        return

    def run(self):
        self.s3.init_s3()

        common_key_prefix = f'jobs/{self.job_id}/intermediate_files'
        input_data_prefix = common_key_prefix + f'/mapper_outputs/shuffler_{self.worker_id}'
        temp_output_prefix = common_key_prefix + f'/shuffler_outputs/temp/shuffler_{self.worker_id}'
        output_key = common_key_prefix + f'/shuffler_outputs/reducer_{self.worker_id}/shuffler_{self.worker_id}.json'

        # Get each mapper output file, sort it and store result
        shuffler_keys = self.s3.stream_keys_in_dir(self.data_bucket, input_data_prefix)
        for key in shuffler_keys:
            # TODO: I should handle this better according to the size of the file
            temp_output_suffix = key.replace(input_data_prefix, '')
            temp_output_key = temp_output_prefix + temp_output_suffix
            self.s3.sort_json(self.data_bucket, key, self.data_bucket, temp_output_key)

        # Merge the sorted files, sorting them (use heapq.merge())
        shuffler_sorted_keys = self.s3.stream_keys_in_dir(self.data_bucket, temp_output_prefix)
        streams = [self.s3.stream_file_pairs(self.data_bucket, key) for key in shuffler_sorted_keys]
        merged_data = heapq.merge(*streams, key=lambda pair: pair[0])

        # Upload final file
        self.s3.upload_sorted_data_in_chunks(self.data_bucket, output_key, merged_data)

        self.s3.close()
        exit(0)
    
class ReduceWorker:
    def __init__(self):
        self.job_id = None
        self.worker_id = None
        self.reduce_fn = None
        self.data_bucket = 'rustfs'
        self.s3 = S3Storage()


    def reduce_lines(self, data):
        previous_key = None
        values_buffer = []
        for key, values in data:
            if key != previous_key and previous_key is not None:
                result_key, result_value = self.reduce_fn((previous_key, values_buffer))

                values_buffer.clear()
                previous_key = key
                yield result_key, result_value

            values_buffer.extend(values)

        # Handle last key
        if previous_key is not None:
            result_key, result_value = self.reduce_fn((previous_key, values_buffer))
            yield result_key, result_value

    def run(self):
        self.s3.init_s3()
        common_key_prefix = f'jobs/{self.job_id}'
        input_data_prefix = common_key_prefix + f'intermediate_files/shuffler_outputs/reducer_{self.worker_id}/shuffler_{self.worker_id}.json'
        output_prefix = common_key_prefix + f'output_files/job_{self.job_id}.json'

        input_lines = self.s3.stream_file_pairs(self.data_bucket, input_data_prefix)
        reducer = self.reduce_lines(input_lines)
        self.s3.upload_sorted_data_in_chunks(self.data_bucket, output_prefix, reducer)

        self.s3.close()
        exit(0)