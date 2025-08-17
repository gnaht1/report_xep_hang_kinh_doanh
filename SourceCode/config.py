import os

# --- Google Drive Configuration ---
# Folder ID for Google Drive where reports are stored
GOOGLE_DRIVE_FOLDER_ID = os.environ.get(
    "GOOGLE_DRIVE_FOLDER_ID", "1-HCjtywB6ROjFLeIlzZ2EYPfDckV7LOR"
)

# Full URL to Google Drive folder - will be used in emails
GOOGLE_DRIVE_FOLDER = os.environ.get(
    "GOOGLE_DRIVE_FOLDER",
    f"https://drive.google.com/drive/folders/{GOOGLE_DRIVE_FOLDER_ID}",
)

# --- Email Notification Configuration ---
ENABLE_EMAIL_NOTIFICATION = (
    os.environ.get("ENABLE_EMAIL_NOTIFICATION", "True").lower() == "true"
)

# Email settings
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "umldoomsday@gmail.com")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "whre fgng xcez xitl")

# Default email for manager/supervisor (now optional since email is provided in UI)
MANAGER_EMAIL = os.environ.get("MANAGER_EMAIL", "nphuthang15@gmail.com")
