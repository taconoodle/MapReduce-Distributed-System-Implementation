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
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0f/Afex8CGzERpi7GnOCLAHD32OWyW0wl1/mgpjtcczqUaKe6bHSTTLGGF9RUfJp+kf3lTF+hTf/+JLI5g+32MxwbqNIYWSN9RYYXtPeuJdXh41qL67e5oExRLvgvP05FY/KBZAkCefnK+wISfb+kMSv+mTwc6+8tZ5pNTLLbkeH1BXCaalMcyrJM7FqlLAOYZ9i86qlwWE4KlMlN3gYdFYc6si4DaRM7bxH++XvVGBLhO/3xHbFq5GhMH2XA0D80WaIaz6uXjQ+rkahyr87TPKMCc4xb3vQb08r+Xyey1q+oYQpyH6dFzqkOQ/cKy1hM4n1mMLY83evjosAYI90/QIDAQAB
-----END PUBLIC KEY-----"""
# Where Keycloak sends the user after a successful login
REDIRECT_URI = "http://localhost:8000/callback"