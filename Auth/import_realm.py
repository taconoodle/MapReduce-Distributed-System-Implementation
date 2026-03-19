import requests
import json
import time

def import_realm():
    
    BASE_URL = "http://keycloak-service:8080" 
    ADMIN_USER = "admin"
    ADMIN_PASS = "admin"
    REALM_NAME = "map-project"
    CONFIG_PATH = "AUTH/realm-config.json"
    
    print(f"Connecting to Keycloak at {BASE_URL}...")
    timeout = 300
    start = time.time()
    while True:
        try:
            # Standard Keycloak health check via the master realm config
            r = requests.get(f"{BASE_URL}/realms/master/.well-known/openid-configuration", timeout=2)
            if r.status_code == 200:
                print("Keycloak is alive.")
                break
        except requests.exceptions.RequestException:
            if time.time() - start > timeout:
                print("ERROR: Keycloak did not start in time.")
                return
            time.sleep(5)
    print("[*] Authenticating with Master Realm...")
    token_res = requests.post(
        f"{BASE_URL}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": ADMIN_USER,
            "password": ADMIN_PASS,
        }
    )
    token_res.raise_for_status() 
    token = token_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Check if realm exists first
    check = requests.get(f"{BASE_URL}/admin/realms/{REALM_NAME}", headers=headers)
    
    if check.status_code == 200:
        print(f"[*] Realm '{REALM_NAME}' already exists. Skipping.")
        return

    # Load your specific JSON from the AUTH folder
    with open(CONFIG_PATH, 'r') as f:
        realm_data = json.load(f)

    # POST to create a NEW realm 
    response = requests.post(f"{BASE_URL}/admin/realms", json=realm_data, headers=headers)
    
    if response.status_code == 201:
        print(f"Successfully imported {REALM_NAME}!")
    else:
        print(f"[ERROR] Import failed: {response.text}")
        
if __name__ == "__main__":
    import_realm()        
        