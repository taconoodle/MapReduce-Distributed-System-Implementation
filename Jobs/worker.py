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
        # Get available mapper output files from S3
        # Find data in RustFS/jobs/{job_id}/intermediate_files/mapper_outputs/shuffler_{shuffler_id}
        input_data_prefix = f'jobs/{self.job_id}/intermediate_files/mapper_outputs/shuffler_{self.shuffler_id}'
        input_data_bucket = 'RustFS'

        shuffler_keys = list()
        for page in self.s3.stream_paginator_pages(input_data_bucket, input_data_prefix):
            for contents in page['Contents']:
                # WARNING: The 'Key' is the whole name of the file, meaning it has the prefix attached as well
                shuffler_keys.append(contents['Key'])


        # Get each mapper output file and sort it. Store result in RustFS/jobs/{job_id}/intermediate_files/shuffler_outputs/temp/shuffler_{shuffler_id}/sorted_mapper_{mapper_id}.json
        temp_output_bucket = 'RustFS'
        temp_output_prefix = f'jobs/{self.job_id}/intermediate_files/shuffler_outputs/temp/shuffler_{self.shuffler_id}'
        for key in shuffler_keys:
            # TODO: Create a wrapper func for this
            file_body = self.s3.conn.get_object(input_data_bucket, key)['Body']
            file_data = json.load(file_body)

            # Sort dict based on keys
            file_data = dict(sorted(file_data.items(), key=lambda item: item[0])) # We were bad but now we're good
            temp_output_suffix = key.replace(input_data_prefix, '')
            temp_output_key = temp_output_prefix + temp_output_suffix
            self.s3.conn.put_object(
                Bucket=temp_output_bucket,
                Key=temp_output_key,
                Body=json.dumps(file_data)
            )

        # Merge the sorted files, sorting them (use heapq.merge())
        shuffler_sorted_keys = list()
        for page in self.s3.stream_paginator_pages(temp_output_bucket, temp_output_prefix):
            for contents in page['Contents']:
                shuffler_keys.append(contents['Key'])

        streams = [self.s3.stream_file_lines(temp_output_bucket, key) for key in shuffler_keys]
        merged_data = heapq.merge(*streams, key=lambda pair: pair[0])

        output_bucket = 'RustFS'
        output_key = f'jobs/{self.job_id}/intermediate_files/shuffler_outputs/reducer_{shuffler_id}/shuffler_{self.shuffler_id}.json'
        mpu = self.s3.conn.create_multipart_upload(
            Bucket=output_bucket,
            Key=output_key
        )
        upload_id = mpu['UploadId']
        parts = []
        part_number = 1
        buffer = []
        buffer_size = 0
        for key, group in groupby(merged_data, key=lambda pair: pair[0]):
            values = [value for _, value in group]
            line = json.dumps({key: values}) + '\n'
            line = line.encode('utf-8')
            buffer_size += len(line)

            if buffer_size >= (5 * (1024 ** 2)):
                chunk =b''.join(buffer)
                response = self.s3.conn.upload_part(
                    Bucket=output_bucket,
                    Key=output_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk
                )
                parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
                part_number += 1
                buffer = []
                buffer_size = 0

        if buffer:
            chunk = b''.join(buffer)
            response = self.s3.conn.upload_part(
                Bucket=output_bucket,
                Key=output_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=chunk
            )
            parts.append({'PartNumber': part_number, 'ETag': response['ETag']})

        self.s3.conn.complete_multipart_upload(
            Bucket=output_bucket,
            Key=output_key,
            UploadId=upload_id,
            MultipartUpload={'Parts':parts}
        )

        # Store the big output file in RustFS/jobs/{job_id}/intermediate_files/shuffler_outputs/reducer_{shuffler_id}/shuffler_{shuffler_id}.json
        exit(0)
    
class ReduceWorker:
    def run(self):
        # Get the file from RustFS/jobs/{job_id}/intermediate_files/shuffler_outputs/reducer_{reducer_id}/
                
        # For each (key, value) pair in the data, append value to key's list
        # If the key is different from the previous, call the reducer function on the previous key and its values
        # Store the result of the reducer function in the results dict
        
        # Dump the dictionary to the JSON output file of the worker
        # Store the output in RustFS/jobs/{job_id}/intermediate_files/reducer_outputs/reducer_{reducer_id}.json
        exit(0)