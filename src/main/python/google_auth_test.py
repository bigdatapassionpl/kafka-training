from google.auth import default
from google.auth.transport.requests import Request


def get_access_token():
    # Get default credentials
    credentials, project = default()

    # Refresh the credentials to get a valid access token
    credentials.refresh(Request())

    return credentials.token


# # Usage
token = get_access_token()
print(f"Access token: {token}")