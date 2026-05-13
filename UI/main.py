import json
import requests
import hashlib
import base64
import secrets
from GUI.utils import get_user_from_cookie
from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse, RedirectResponse, HTMLResponse
from GUI.constants import AUTH_URL, CLIENT_ID, REDIRECT_URI, TOKEN_URL
from jose import jwt

app = FastAPI()
MANAGER_URL = "http://manager-service:8000"

# --- CLI SESSION STORAGE ---
cli_sessions = {} 

# Mount the templates folder
app.mount("/static", StaticFiles(directory="GUI/templates"), name="static")

# --- AUTHENTICATION FLOW ---

@app.get("/")
async def root(request: Request, state: str = None):
    if request.cookies.get("auth_token"):
        return RedirectResponse(url="/dashboard")
    if state:
        return RedirectResponse(url=f"/login?state={state}")
    return FileResponse('GUI/templates/login.html')

@app.get("/login")
async def login_trigger(state: str = None):
    
    code_verifier = secrets.token_urlsafe(64)
    
    
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).decode().replace('=', '')


    target = (
        f"{AUTH_URL}?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&scope=openid"
        f"&redirect_uri={REDIRECT_URI}"
        f"&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
    )
    
    if state:
        target += f"&state={state}"
    
    response = RedirectResponse(url=target)
    
    response.set_cookie(key="pkce_verifier", value=code_verifier, httponly=True, max_age=300)
    return response

@app.get("/callback")
async def auth_callback(request: Request, code: str, state: str = None):
    # take verifier from cookie 
    code_verifier = request.cookies.get("pkce_verifier")
    
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "code_verifier": code_verifier,
    }
    
    # exchange code with token
    keycloak_res = requests.post(TOKEN_URL, data=data)
    token_data = keycloak_res.json()
    access_token = token_data.get("access_token")

    if not access_token:
        print(f"Auth Failed: {token_data}")
        return JSONResponse(status_code=400, content={"message": "Failed to get token"})

    
    if state and state != "undefined":
        cli_sessions[state] = access_token
        return HTMLResponse(content="""
            <html>
                <body style="text-align:center; padding-top:100px; background:#1e1e2f; color:#ffffff; font-family:sans-serif;">
                    <div style="border:2px solid #4caf50; display:inline-block; padding:20px; border-radius:10px;">
                        <h1 style="color:#4caf50;">✅ Login Successful!</h1>
                        <p style="font-size:1.2em;">The CLI has been authenticated.</p>
                        <p style="color:#aaa;">This window will close automatically in 3 seconds...</p>
                    </div>
                    <script>
                        setTimeout(function() { window.close(); }, 3000);
                    </script>
                </body>
            </html>
        """)
    # --------------------------------------

    
    response = RedirectResponse(url="/dashboard")
    

    response.set_cookie(
        key="auth_token", 
        value=access_token, 
        httponly=True, 
        secure=False, 
        samesite="lax"
    )
    

    response.delete_cookie("pkce_verifier")
    
    return response

# --- CLI POLLING ENDPOINT (NEW) ---
@app.get("/api/cli/check/{state}")
async def check_cli_auth(state: str):
    if state in cli_sessions:
        token = cli_sessions.pop(state)
        return {"status": "success", "access_token": token}
    return JSONResponse(status_code=404, content={"status": "pending"})

# --- NAVIGATION ---

@app.get("/dashboard")
async def dashboard(request: Request):
    user = await get_user_from_cookie(request)
    if not user:
        return RedirectResponse(url="/")
    return FileResponse('GUI/templates/dashboard.html')

@app.get("/admin")
async def admin_portal(request: Request):
    user = await get_user_from_cookie(request)
    if not user or not user.get("is_admin"):
        return RedirectResponse(url="/dashboard?error=unauthorized")
    return FileResponse('GUI/templates/admin_dashboard.html')

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/")
    response.delete_cookie("auth_token")
    return response

@app.get("/results")
async def results_page(request: Request):
    if not request.cookies.get("auth_token"):
        return RedirectResponse(url="/")
    return FileResponse('GUI/templates/results.html')

# --- UNIVERSAL API HELPER ---
def get_token_from_request(request: Request):
    # Προσπαθεί να βρει το token είτε σε Cookie (Browser) είτε σε Header (CLI)
    token = request.cookies.get("auth_token")
    if not token:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
    return token

# --- API ENDPOINTS ---

@app.post("/api/submit-job")
async def submit_job(request: Request, file: UploadFile = File(...)):
    """
    job submission process:
    1. validates the user JWT
    2. requests URL from the manager
    3. proxies the file upload
    4. triggers the Job start on the manager
    """
    
    # check user token
    token = get_token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Authentication token missing")

    try:
        # extract user info from the JWT 
        payload = jwt.get_unverified_claims(token)
        user_id = payload.get("sub")
        username = payload.get("preferred_username")
        
        # payload for the manager
        submit_payload = {
            "user_id": user_id,
            "file_name": file.filename,
            "file_size": file.size if file.size else 0,
            "part_size": 5 * 1024 * 1024  # Standard 5MB chunk size for multipart
        }
        
        # requesting manager url to start the process
        manager_res = requests.post(f"{MANAGER_URL}/jobs/submit", json=submit_payload)
        manager_res.raise_for_status()
        job_data = manager_res.json()
        
        job_id = job_data.get("job_id")
        upload_url = job_data.get("upload_url")  # URL for PUT request

        # proxy the file
        file_content = await file.read()
        s3_res = requests.put(upload_url, data=file_content)
        s3_res.raise_for_status()
    
        # trigger workers
        start_payload = {"user_id": user_id}
        start_res = requests.post(
            f"{MANAGER_URL}/jobs/{job_id}/start", 
            json=start_payload
        )
        start_res.raise_for_status()

        return {
            "status": "success",
            "message": f"Job {job_id} submitted by {username} is now queued",
            "job_id": job_id
        }

    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    except Exception as e:
        print(f"❌ Error during submission: {e}")
        return JSONResponse(status_code=500, content={"message": "Internal Proxy Error"})

@app.get("/api/job-status/{job_id}")
async def check_status(job_id: str, request: Request):
    token = get_token_from_request(request)
    if not token:
        return JSONResponse(status_code=401, content={"message": "Unauthorized"})

    try:
        # proxy the request to the manager
        res = requests.get(f"{MANAGER_URL}/jobs/{job_id}")
        res.raise_for_status()
        
        
        return res.json()
    except Exception:
        return {"status": "WAITING"}

@app.get("/api/download/{job_id}")
async def download_result(job_id: str, request: Request):
    token = get_token_from_request(request)
    if not token:
        return JSONResponse(status_code=401, content={"message": "Unauthorized"})
        
    manager_url = f"http://manager-service:8000/internal/results/{job_id}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        # ask the manager for the job status and the result URL
        res = requests.get(f"{MANAGER_URL}/jobs/{job_id}")
        res.raise_for_status()
        job_info = res.json()

        if job_info.get("status") != "Succeeded":
            return JSONResponse(status_code=400, content={"message": "Job not finished yet"})

        # get the url 
        s3_presigned_url = job_info.get("result_url")
        
        if not s3_presigned_url:
            return JSONResponse(status_code=404, content={"message": "Download link not found"})

        # proxy the download 
        response = requests.get(s3_presigned_url, stream=True, timeout=15)
        
        return StreamingResponse(
            response.iter_content(chunk_size=1024 * 1024), # 1MB chunks
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=results_{job_id}.json"}
        )

    except Exception as e:
        print(f"Download Error: {e}")
        return JSONResponse(status_code=500, content={"message": f"failed to retrieve results: {str(e)}"})