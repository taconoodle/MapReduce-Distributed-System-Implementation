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
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAobN0iAz3VEuzEIT9Zb7br+vGjliy5MmWNlFaI4Yzmn6LCxDju87RrLFFHzVKywlDW9hsKNd8+dSifq1NOJxMf1nLYawmsjC0qJsFC5KSOs5ml7OL+tq+s8wfkzCWzc7JPi+G4UxSP/kLqOLvVSBz8DZcnj8oyb0gJrtvFC9z3G6TxCNYVvKls8pOV0Ru12cyZtk53eY2CK76+og1He6i+xjITOie3XFA5E/chXIfu0L4G0syCcPlBiSJRI6PfBSetuXiz53gQQp46YFYdSFO2bzX+C3l18+GsjecCTuLFAA1PM6RoZsa+BrbYUsIYnc6g+Bwcc5Mmc9+2vFHsWXygQIDAQAB
-----END PUBLIC KEY-----"""
# Where Keycloak sends the user after a successful login
REDIRECT_URI = "http://localhost:8000/callback"