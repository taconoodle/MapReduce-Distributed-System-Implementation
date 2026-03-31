from Manager.storage import S3Storage
from collections import defaultdict
import json

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
            # Store each json file to RustFS/jobs/{job_id}/intermediate_files/mapper_outputs/shuffler_{shuffler_id}/mapper_{mapper_id}.json
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
    def run(self):
        # Find data in RustFS/jobs/{job_id}/intermediate_files/mapper_outputs/shuffler_{shuffler_id}
        
        # Get each mapper output file and sort it. Store result in RustFS/jobs/{job_id}/intermediate_files/shuffler_outputs/temp/shuffler_{shuffler_id}/sorted_mapper_{mapper_id}.json
        # Merge the sorted files, sorting them (use heapq.merge())
        # Store the big output file in RustFS/jobs/{job_id}/intermediate_files/shuffler_outputs/reducer_{shuffler_id}/shuffler_{shuffler_id}.json
        exit(0)
    
class ReduceWorker:
    def run(self):
        # Get the file from RustFS/jobs/{job_id}/intermediate_files/shuffler_outputs/reducer_{reducer_id}/
                
        # For each (key, value) pair in the data, append value to key's list
        # If the key is different than the previous, call the reducer function on the previous key and its values
        # Store the result of the reducer function in the results dict
        
        # Dump the dictionary to the json output file of the worker
        # Store the output in RustFS/jobs/{job_id}/intermediate_files/reducer_outputs/reducer_{reducer_id}.json
        exit(0)