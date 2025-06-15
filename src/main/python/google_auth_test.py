from google.auth import default
from google.auth.transport.requests import Request


def get_access_token():
    # Get default credentials
    credentials, project = default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    # Refresh the credentials to get a valid access token
    credentials.refresh(Request())

    return credentials.token


def get_access_token_with_quota(quota_project_id):
    credentials, project = default(quota_project_id=quota_project_id)
    credentials.refresh(Request())
    return credentials.token


# # Usage
token = get_access_token()
print(f"Access token: {token}")