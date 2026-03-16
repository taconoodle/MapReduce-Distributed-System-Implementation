import json
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
import requests

app = FastAPI()

# Mount the templates folder so the browser can see your HTML
app.mount("/static", StaticFiles(directory="templates"), name="static")

@app.get("/")
async def root():
    return FileResponse('templates/login.html')

@app.get("/dashboard")
async def dashboard():
    return FileResponse('templates/dashboard.html')

@app.get("/results")
async def results_page():
    return FileResponse('templates/results.html')

@app.post("/api/submit-job")
async def handle_job_upload(request: Request):
    print("\n[STEP 1] Received a request at /api/submit-job") # Terminal check
    
    try:
        # 1. Grab the form data (doesn't care about the key name)
        form = await request.form()
        if not form:
            print("[ERROR] Form data is empty!")
            return {"status": "error", "message": "No data received"}

        # Get the first file uploaded
        file = next(iter(form.values())) 
        print(f"[STEP 2] Found file: {file.filename}")

        # 2. Check for .json extension
        if not file.filename.endswith('.json'):
            print(f"[ERROR] User uploaded a non-JSON file: {file.filename}")
            return {"status": "error", "message": "Invalid format. Please upload a .json file."}

        # 3. Read and Parse the JSON
        content = await file.read()
        job_data = json.loads(content)
        print(f"[STEP 3] JSON parsed successfully for job: {job_data.get('job_name', 'Unnamed Job')}")

        # 4. MANAGER HANDSHAKE
        manager_url = "http://manager-service:8000/jobs" 
        print(f"[STEP 4] Attempting to contact Manager at {manager_url}...")
        
        try:
            # Send the data to the Manager with a 5-second timeout
            response = requests.post(manager_url, json=job_data, timeout=5)
            print(f"[SUCCESS] Manager responded with code: {response.status_code}")
            return response.json() 
                
        except Exception as e:
            # This is the "Simulation" block that runs when the Manager isn't there
            print(f"!!! DEBUG: Manager at {manager_url} is unreachable.")
            print(f"!!! REASON: {str(e)}")
            return {
                "status": "success", 
                "message": f"Job '{file.filename}' received by UI. (Manager Offline Simulation)"
            }
            
    except Exception as e:
        print(f"[CRITICAL ERROR] Middleware failed: {str(e)}")
        return {"status": "error", "message": f"Middleware Error: {str(e)}"}
   

@app.get("/api/job-status/{job_id}")
async def check_status(job_id: str):
    # K8s Service DNS name
    manager_url = f"http://manager-service:8000/jobs/{job_id}"
    
    try:
        print(f"[UI] Checking status for job {job_id} via Manager...")
        response = requests.get(manager_url, timeout=3)
        
        # If Manager found the job, return the status (e.g., RUNNING, COMPLETED)
        if response.status_code == 200:
            return response.json()
        else:
            return {"status": "UNKNOWN", "message": "Job ID not found in Manager database"}

    except Exception as e:
        print(f"[ERROR] Could not reach Manager: {e}")
        # Simulation: If manager is down, we pretend it's still processing
        return {"status": "WAITING", "message": "Manager temporarily unreachable"}

@app.get("/api/download/{job_id}")
async def download_result(job_id: str):
    # The UI doesn't look at files! It just asks the Manager to "Hand it over"
    manager_url = f"http://manager-service:8000/internal/results/{job_id}"
    
    try:
        # We use stream=True so we don't load a huge file into memory all at once
        response = requests.get(manager_url, stream=True, timeout=10)
        
        if response.status_code == 200:
            
            
            # This "pipes" the data from the Manager straight to the User's browser
            return StreamingResponse(
                response.iter_content(chunk_size=1024),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename=result_{job_id}.json"}
            )
        else:
            return JSONResponse(status_code=404, content={"message": "Manager says file not ready"})

    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Connection to Manager failed: {e}"})