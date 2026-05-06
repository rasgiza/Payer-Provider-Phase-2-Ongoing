"""
Fabric REST API Authentication Helper
Authenticates with Microsoft Fabric using Azure AD and returns access token.

Shared by all deploy/ and orchestration/ scripts.
"""

from azure.identity import InteractiveBrowserCredential
import sys

def get_fabric_token(tenant_id: str, account_hint: str = None):
    """
    Get access token for Microsoft Fabric REST APIs.

    Args:
        tenant_id: Azure AD tenant ID
        account_hint: Optional account email hint for browser authentication

    Returns:
        Access token string for Fabric API calls
    """
    try:
        FABRIC_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

        credential_kwargs = {"tenant_id": tenant_id}
        if account_hint:
            credential_kwargs["login_hint"] = account_hint

        credential = InteractiveBrowserCredential(**credential_kwargs)

        print(f"Authenticating with Azure AD (tenant: {tenant_id})...")
        token = credential.get_token(FABRIC_SCOPE)

        print("  [OK] Authentication successful")
        return token.token

    except Exception as e:
        print(f"  [FAIL] Authentication failed: {str(e)}")
        sys.exit(1)


def get_auth_headers(token: str):
    """
    Create HTTP headers for Fabric REST API requests.

    Args:
        token: Access token from get_fabric_token()

    Returns:
        Dictionary of HTTP headers
    """
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


if __name__ == "__main__":
    from config import TENANT_ID, ADMIN_ACCOUNT
    ACCOUNT = ADMIN_ACCOUNT

    token = get_fabric_token(TENANT_ID, ACCOUNT)
    print(f"\n  [OK] Token acquired (length: {len(token)} chars)")
    print(f"  [OK] Headers ready: {list(get_auth_headers(token).keys())}")
