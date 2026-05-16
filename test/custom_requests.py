import requests
import os

URL = 'http://localhost:8000'
MB = 1024 ** 2
UPLOAD_PART_SIZE = 0.5 * MB

def connect():
    print(requests.get(f'{URL}').json())

def submit_job():
    user_id = 1
    file_name = 'sample-data.jsonl'
    file_size = os.path.getsize(f'../{file_name}')
    part_size = 3 * 1024

    url = f'{URL}/jobs/submit'
    data = {
        'user_id': user_id,
        'file_name': file_name,
        'file_size': file_size,
        'part_size': part_size
    }

    response = requests.post(
        url,
        json=data
    )
    print(response.json())
    return response.json()

def upload_bytes(url, data):
    return requests.put(url, data=data)

def init_job():
    new_job = submit_job()
    job_id = new_job['job_id']
    upload_url = new_job['upload_url']
    map_url = new_job['map_url']
    reduce_url = new_job['reduce_url']

    with open('../sample-data.jsonl', 'rb') as f:
        data = f.read()
        upload_response = upload_bytes(upload_url, data)
        print(f'Data upload: {upload_response}')

    with open('../map_fn.pkl', 'rb') as f:
        data = f.read()
        upload_response = upload_bytes(map_url, data)
        print(f'Map upload: {upload_response}')

    with open('../reduce_fn.pkl', 'rb') as f:
        data = f.read()
        upload_response = upload_bytes(reduce_url, data)
        print(f'Reduce upload: {upload_response}')

    url = f'{URL}/jobs/{job_id}/start'
    response = requests.post(url, json={'user_id': 1})
    return response


connect()
print(f'Init job result: {init_job()}')