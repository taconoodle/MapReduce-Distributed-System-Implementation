# Keycloak Server URL
KC_SERVER_URL = "http://localhost:8080"
REALM_NAME = "map-project"
CLIENT_ID  = "map-reduce-ui"
# These are the standard OIDC URLs the UI needs 
BASE_OIDC_URL = f"{KC_SERVER_URL}/realms/{REALM_NAME}/protocol/openid-connect"
AUTH_URL    = f"{BASE_OIDC_URL}/auth"
TOKEN_URL   = f"{BASE_OIDC_URL}/token"
USERINFO_URL = f"{BASE_OIDC_URL}/userinfo"
LOGOUT_URL  = f"{BASE_OIDC_URL}/logout"

# Where Keycloak sends the user after a successful login
REDIRECT_URI = "http://localhost:3000/callback"