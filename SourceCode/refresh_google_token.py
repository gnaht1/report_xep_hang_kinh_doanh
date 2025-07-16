"""
Utility script to refresh or regenerate Google API token
Run this script when you encounter authentication errors with Google Drive

Usage:
    python refresh_google_token.py
"""

import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Define the OAuth 2.0 scopes that your application requests
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def refresh_google_token():
    """
    Interactive utility to refresh or regenerate the Google API token
    """
    print("Google Token Refresh Utility")
    print("============================")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(script_dir, "token.json")
    credential_path = os.path.join(script_dir, "credentials.json")

    # Check for credentials.json
    if not os.path.exists(credential_path):
        print("\n❌ Error: 'credentials.json' not found!")
        print("Please download credentials.json from Google Cloud Console:")
        print("1. Go to https://console.cloud.google.com/apis/credentials")
        print("2. Create or select an OAuth 2.0 Client ID")
        print("3. Download the JSON and save as 'credentials.json' in this directory")
        return False

    # Check token status
    creds = None
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            print(f"\nℹ️ Found existing token ({'valid' if creds.valid else 'invalid'})")

            # Try to refresh if expired but has refresh token
            if creds.expired and creds.refresh_token:
                print("🔄 Token expired. Attempting to refresh...")
                try:
                    creds.refresh(Request())
                    print("✅ Token refreshed successfully!")
                    # Save the refreshed token
                    with open(token_path, "w") as token_file:
                        token_file.write(creds.to_json())
                    return True
                except Exception as e:
                    print(f"❌ Token refresh failed: {e}")
                    print("\nCreating new token...")
                    creds = None
        except Exception as e:
            print(f"\n⚠️ Error reading token: {e}")
            print("Will create a new token.")
            creds = None
    else:
        print("\nNo token.json found. Will create new token.")

    # If we reach here, we need to create a new token
    if not creds or not creds.valid:
        try:
            # Start OAuth 2.0 flow
            print("\n🔒 Starting authentication flow...")
            flow = InstalledAppFlow.from_client_secrets_file(credential_path, SCOPES)
            creds = flow.run_local_server(port=0)

            # Save the obtained credentials
            with open(token_path, "w") as token_file:
                token_file.write(creds.to_json())
            print("\n✅ Authentication successful! New token saved.")

            return True
        except Exception as e:
            print(f"\n❌ Authentication failed: {e}")
            return False


if __name__ == "__main__":
    if refresh_google_token():
        print("\n✨ Token is now valid and ready to use!")
        print("You can now run your application again.")
    else:
        print("\n❌ Token refresh/creation failed. Please check the errors above.")
