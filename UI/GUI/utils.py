from jose import jwt
from constants import PUBLIC_KEY, CLIENT_ID
from fastapi import Request

async def get_user_from_cookie(request: Request):
    token = request.cookies.get("auth_token")
    if not token:
        return None
    try:
        payload = jwt.decode(token, PUBLIC_KEY, algorithms=["RS256"], audience=CLIENT_ID)
        roles = payload.get("realm_access", {}).get("roles", [])
        return {
            "username": payload.get("preferred_username"),
            "is_admin": "app-admin" in roles
        }
    except Exception:
        return None