import requests
import os
import subprocess
import time
import requests

CURRENT_SYSTEM = os.name


def start_port_forward(service, local_port=8000, remote_port=8000, namespace="default"):
    """Start kubectl port-forward as a background process."""
    cmd = [
        "wsl", "kubectl", "port-forward",
        f"services/{service}",
        f"{local_port}:{remote_port}"
    ] if CURRENT_SYSTEM == 'nt' else [
        "kubectl", "port-forward",
        f"services/{service}",
        f"{local_port}:{remote_port}"
    ]


    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    # time.sleep(2)  # Give it a moment to establish the tunnel
    return proc

def stop_port_forward(proc):
    """Terminate the port-forward process."""
    if proc and proc.poll() is None:
        proc.terminate()
        proc.wait()

URL = 'http://localhost:8000'
MB = 1024 ** 2
UPLOAD_PART_SIZE = 0.5 * MB

def connect():
    print(requests.get(f'{URL}').json())

def submit_job():
    user_id = '1'
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
    print(response)
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


# --- Usage ---

manager_proc = start_port_forward("manager-service")  # replace with your service name
rustfs_proc = start_port_forward("rustfs-service", 9000, 9000)  # replace with your service name
rustfs_ui_proc = start_port_forward("rustfs-service", 9001, 9001)  # replace with your service name
time.sleep(1)
try:
    connect()
    print(f'Init job result: {init_job()}')
finally:
    stop_port_forward(manager_proc)
    stop_port_forward(rustfs_proc)
    stop_port_forward(rustfs_ui_proc)