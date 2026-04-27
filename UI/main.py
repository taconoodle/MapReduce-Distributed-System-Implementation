import json
import requests
import hashlib
import base64
import secrets
from GUI.utils import get_user_from_cookie
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse, RedirectResponse, HTMLResponse
from GUI.constants import AUTH_URL, CLIENT_ID, REDIRECT_URI, TOKEN_URL

app = FastAPI()

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
    # 1. Δημιουργούμε έναν τυχαίο Code Verifier
    code_verifier = secrets.token_urlsafe(64)
    
    # 2. Δημιουργούμε το Code Challenge (SHA256 hash)
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).decode().replace('=', '')

    # 3. Φτιάχνουμε το URL με τις PKCE παραμέτρους
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
    
    # 4. Αποθηκεύουμε τον verifier σε ένα προσωρινό cookie για το επόμενο βήμα
    response.set_cookie(key="pkce_verifier", value=code_verifier, httponly=True, max_age=300)
    return response

@app.get("/callback")
async def auth_callback(request: Request, code: str, state: str = None):
    # 1. Παίρνουμε τον verifier από το cookie
    code_verifier = request.cookies.get("pkce_verifier")
    
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "code_verifier": code_verifier,
    }
    
    # 2. Ανταλλαγή code με token
    # Χρησιμοποιούμε άλλο όνομα (keycloak_res) για να μη μπερδευτεί με το Response της FastAPI
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

    # 3. Προετοιμασία της απάντησης στον Browser
    response = RedirectResponse(url="/dashboard")
    
    # Θέτουμε το cookie για το GUI
    response.set_cookie(
        key="auth_token", 
        value=access_token, 
        httponly=True, 
        secure=False, 
        samesite="lax"
    )
    
    # Καθαρίζουμε το PKCE cookie
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
async def handle_job_upload(request: Request):
    token = get_token_from_request(request)
    if not token:
        return JSONResponse(status_code=401, content={"message": "Authentication required"})
    
    try:
        form = await request.form()
        if not form:
            return {"status": "error", "message": "No data received"}

        file = next(iter(form.values())) 
        if not file.filename.endswith('.json'):
            return {"status": "error", "message": "Invalid format. Please upload a .json file."}

        content = await file.read()
        job_data = json.loads(content)

        manager_url = "http://manager-service:8000/jobs" 
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(manager_url, json=job_data, headers=headers, timeout=5)
            return response.json() 
        except Exception as e:
            return {"status": "success", "message": f"Job received (Manager offline simulation)"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/job-status/{job_id}")
async def check_status(job_id: str, request: Request):
    token = get_token_from_request(request)
    if not token:
        return JSONResponse(status_code=401, content={"message": "Unauthorized"})
    
    manager_url = f"http://manager-service:8000/jobs/{job_id}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(manager_url, headers=headers, timeout=3)
        return response.json() if response.status_code == 200 else {"status": "UNKNOWN"}
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
        response = requests.get(manager_url, headers=headers, stream=True, timeout=10)
        if response.status_code == 200:
            return StreamingResponse(
                response.iter_content(chunk_size=1024),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename=result_{job_id}.json"}
            )
        return JSONResponse(status_code=404, content={"message": "File not ready"})
    except Exception as e:
        return JSONResponse(status_code=500, ncotent={"message": str(e)})