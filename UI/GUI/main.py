import json
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse, RedirectResponse
from constants import AUTH_URL, CLIENT_ID, REDIRECT_URI, TOKEN_URL
import requests

app = FastAPI()

# Mount the templates folder so the browser can see HTML
app.mount("/static", StaticFiles(directory="templates"), name="static")

@app.get("/")
async def root(request: Request):
    # If the cookie exists, skip login and go to dashboard
    if request.cookies.get("auth_token"):
        return RedirectResponse(url="/dashboard")
    return FileResponse('templates/login.html')

@app.get("/login")
async def login_trigger():
    target = f"{AUTH_URL}?client_id={CLIENT_ID}&response_type=code&scope=openid&redirect_uri={REDIRECT_URI}"
    return RedirectResponse(url=target)

@app.get("/callback")
async def auth_callback(code: str):
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
    }
    
    # 1. Exchange code for token
    response = requests.post(TOKEN_URL, data=data)
    token_data = response.json()
    access_token = token_data.get("access_token")

    if not access_token:
        return JSONResponse(status_code=400, content={"message": "Failed to get token"})

    # 2. Create the redirect response
    response = RedirectResponse(url="/dashboard")

    # 3. SET THE COOKIE 
    response.set_cookie(
        key="auth_token", 
        value=access_token, 
        httponly=True,   # XSS not allowed now
        max_age=3600,    # Token expires in 1 hour
        samesite="lax"   # Security best practice
    )
    
    return response

@app.get("/dashboard")
async def dashboard(request: Request):
    if not request.cookies.get("auth_token"):
        return RedirectResponse(url="/")
    return FileResponse('templates/dashboard.html')

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/")
    response.delete_cookie("auth_token")
    return response

@app.get("/results")
async def results_page(request: Request):
    if not request.cookies.get("auth_token"):
        return RedirectResponse(url="/")
    return FileResponse('templates/results.html')

@app.post("/api/submit-job")
async def handle_job_upload(request: Request):
    # Security check
    token = request.cookies.get("auth_token")
    if not token:
        return JSONResponse(status_code=401, content={"message": "Authentication required"})
    
    try:
        # 1. Grab the form data 
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
        print(f"JSON parsed successfully for job: {job_data.get('job_name', 'Unnamed Job')}")

        # 4. MANAGER HANDSHAKE
        manager_url = "http://manager-service:8000/jobs" 
        print(f"Attempting to contact Manager at {manager_url}...")
        headers = {
            "Authorization": f"Bearer {token}",  # Pass identity to Manager
            "Content-Type": "application/json"
        }
        try:
            # Send the data to the Manager with a 5-second timeout
            response = requests.post(manager_url, json=job_data, headers=headers, timeout=5)
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
async def check_status(job_id: str, request: Request):
    
    # Security check
    token = request.cookies.get("auth_token")
    if not token:
        return JSONResponse(status_code=401, content={"message": "Unauthorized"})
    
    # K8s Service DNS name
    manager_url = f"http://manager-service:8000/jobs/{job_id}"
    headers = {
        "Authorization": f"Bearer {token}",  # Pass identity to Manager
        "Content-Type": "application/json"
    }
    try:
        print(f"[UI] Checking status for job {job_id} via Manager...")
        response = requests.get(manager_url, headers=headers,timeout=3)
        
        # If Manager found the job, return the status (e.g., RUNNING, COMPLETED)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            return JSONResponse(status_code=401, content={"message": "Manager rejected session"})
        else:
            return {"status": "UNKNOWN", "message": "Job ID not found"}

    except Exception as e:
        print(f"[ERROR] Could not reach Manager: {e}")
        return {"status": "WAITING", "message": "Manager temporarily unreachable"}

@app.get("/api/download/{job_id}")
async def download_result(job_id: str, request: Request):
    # Security check
    token = request.cookies.get("auth_token")
    if not token:
        return JSONResponse(status_code=401, content={"message": "Unauthorized"})
    # Ask manager to give job result
    manager_url = f"http://manager-service:8000/internal/results/{job_id}"
    headers = {
        "Authorization": f"Bearer {token}",  # Pass identity to Manager
        "Content-Type": "application/json"
    }
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