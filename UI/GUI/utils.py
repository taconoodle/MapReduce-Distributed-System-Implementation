from jose import jwt
from GUI.constants import PUBLIC_KEY, CLIENT_ID
from fastapi import Request

async def get_user_from_cookie(request: Request):
    token = request.cookies.get("auth_token")
    if not token:
        return None
    try:
        # 1. Δες τι έχει μέσα το token χωρίς verification
        unverified = jwt.get_unverified_claims(token)
        print(f"DEBUG TOKEN: {unverified}") 

        payload = jwt.decode(token, PUBLIC_KEY, algorithms=["RS256"], options={"verify_aud": False})
        
        # 2. Εδώ είναι το κρίσιμο σημείο: Πού είναι τα roles;
        # Το Keycloak συνήθως τα βάζει στο 'realm_access' -> 'roles'
        realm_access = payload.get("realm_access", {})
        roles = realm_access.get("roles", [])
        
        print(f"DEBUG ROLES FOUND: {roles}")

        is_admin = "map_admin" in roles
        return {
            "username": payload.get("preferred_username"),
            "is_admin": is_admin
        }
    except Exception as e:
        print(f"JWT ERROR: {e}")
        return None