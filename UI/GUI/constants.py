# Keycloak Server URL
KC_SERVER_URL = "http://localhost:8080"
REALM_NAME = "map-project"
CLIENT_ID  = "map-reduce-client"
# These are the standard OIDC URLs the UI needs 
BASE_OIDC_URL = f"{KC_SERVER_URL}/realms/{REALM_NAME}/protocol/openid-connect"
AUTH_URL    = f"{BASE_OIDC_URL}/auth"
TOKEN_URL   = f"{BASE_OIDC_URL}/token"
USERINFO_URL = f"{BASE_OIDC_URL}/userinfo"
LOGOUT_URL  = f"{BASE_OIDC_URL}/logout"
PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvE9kWV12RCVs6KrijLN3
eNQQHLbcK6LTXoF3j1Z0tX/YHRAnVoQ+GNVUXIBkYsjRbWNrdp/MC73CGaB99Qui
0apznePVoPt4iwxdEN04blXFGQgBAoFHFLSoEVrg3VxnFafmhuFpyp5sGgqVLxqk
ZvtkQCM9yupPXOlAKSHLrVRsJ1rVh/kzpIlMX8zbknt+/rdcl2s1usEnYUEC85QV
HSSxq6Db8lT/ih9N8drJlIh7mVY9PRVKn217kff3QiJrbbhqSW+R17waMhc6KqT1
bfO/8oYkN1+Op6peNyyvUapcCoi0vcTncKPIjVDJ3DmyWZ+GKkvQIWxOwLF7GNUV
PwIDAQAB
-----END PUBLIC KEY-----"""
# Where Keycloak sends the user after a successful login
REDIRECT_URI = "http://localhost:8000/callback"