from Manager.storage import S3Storage
from collections import defaultdict
import json
import heapq
from itertools import groupby

class MapWorker:
    def __init__(self):
        self.chunk_name = None
        self.job_id = None
        self.map_fn = None
        self.worker_id = None
        self.worker_num = None
        self.s3 = S3Storage()
        
    def get_chunk(self):
        self.s3.init_s3()
        chunk_path = f'rustfs/jobs/{self.job_id}/intermediate_files/chunks'
        self.s3.download_from_bucket(chunk_path, self.chunk_name, f'/data/{self.chunk_name}')
        self.s3.close()
        
            
    def run(self):
        # Get the file chunk from RustFS
        # TODO: I should turn this to a get_object()['Body'] call
        self.get_chunk()
        chunk_local_path = f'/data/{self.chunk_name}'

        with open(chunk_local_path, 'r') as file:
            data = json.load(file)

        # For each part of the chunk, perform the map function
        results = defaultdict(lambda: defaultdict(list))
        for item in data:
            kv_pairs = self.map_fn(item)
            for key, value in kv_pairs:
                # Hash the key of each of the key, value pairs that come out of the map function
                shuffler_id = self.get_responsible_shuffler(key)
                # Store the (key, value) pair in the respective dictionary entry of the shuffler that corresponds to the key
                results[shuffler_id][key].extend(value)

        # Write the dictionary entry of each reducer to a json file
        shuffler_data_path = f'rustfs/jobs/{self.job_id}/intermediate_files/mapper_outputs/'
        for shuffler_id, shuffler_data in results:
            # Store each JSON file to RustFS/jobs/{job_id}/intermediate_files/mapper_outputs/shuffler_{shuffler_id}/mapper_{mapper_id}.json
            # TODO: That's wrong, I should upload it to s3
            with open(f'{shuffler_data_path}/shuffler_{shuffler_id}/mapper_{self.worker_id}.json', 'w') as file:
                json.dump(shuffler_data, file)

        exit(0)
    
    
    def get_responsible_shuffler(self, key):
        # Hash the key
        key_hash = hash(key)
        # Mod the hash with the number of reducers
        shuffler_id = key_hash % self.worker_num
        # Return the number of the reducer that will handle this key
        return shuffler_id
    
class ShuffleWorker:
    def __init__(self):
        self.job_id = None
        self.shuffler_id = None
        self.s3 = S3Storage()
        return

    def run(self):
        data_bucket = 'RustFS'
        common_key_prefix = f'jobs/{self.job_id}/intermediate_files'
        input_data_prefix = common_key_prefix + f'/mapper_outputs/shuffler_{self.shuffler_id}'
        temp_output_prefix = common_key_prefix + f'/shuffler_outputs/temp/shuffler_{self.shuffler_id}'
        output_key = common_key_prefix + f'/shuffler_outputs/reducer_{self.shuffler_id}/shuffler_{self.shuffler_id}.json'

        # Get each mapper output file, sort it and store result
        shuffler_keys = self.s3.stream_keys_in_dir(data_bucket, input_data_prefix)
        for key in shuffler_keys:
            # TODO: I should handle this better since I don't know the size of the file
            temp_output_suffix = key.replace(input_data_prefix, '')
            temp_output_key = temp_output_prefix + temp_output_suffix
            self.s3.sort_json(data_bucket, key, data_bucket, temp_output_key)

        # Merge the sorted files, sorting them (use heapq.merge())
        shuffler_sorted_keys = self.s3.stream_keys_in_dir(data_bucket, temp_output_prefix)
        streams = [self.s3.stream_file_pairs(data_bucket, key) for key in shuffler_sorted_keys]
        merged_data = heapq.merge(*streams, key=lambda pair: pair[0])

        # Upload final file
        self.s3.upload_sorted_data_in_chunks(data_bucket, output_key, merged_data)
        exit(0)
    
class ReduceWorker:
    def __init__(self):
        self.job_id = None
        self.reducer_id = None
        self.reduce_fn = None
        self.s3 = S3Storage()


    def reduce_lines(self, data):
        results = dict()
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
        # Get the file from RustFS/jobs/{job_id}/intermediate_files/shuffler_outputs/reducer_{reducer_id}/
        data_bucket = 'RustFS'
        common_key_prefix = f'jobs/{self.job_id}'
        input_data_prefix = common_key_prefix + f'intermediate_files/shuffler_outputs/reducer_{self.reducer_id}/shuffler_{self.reducer_id}.json'
        output_prefix = common_key_prefix + f'output_files/job_{self.job_id}.json'

        input_lines = self.s3.stream_file_pairs(data_bucket, input_data_prefix)
        reducer = self.reduce_lines(input_lines)
        self.s3.upload_sorted_data_in_chunks(data_bucket, output_prefix, reducer)

        exit(0)