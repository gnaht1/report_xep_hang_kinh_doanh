import os

# --- Google Drive Configuration ---
GOOGLE_DRIVE_FOLDER_ID = os.environ.get(
    "GOOGLE_DRIVE_FOLDER_ID", "1-HCjtywB6ROjFLeIlzZ2EYPfDckV7LOR"
)

# --- Email Notification Configuration ---
ENABLE_EMAIL_NOTIFICATION = (
    os.environ.get("ENABLE_EMAIL_NOTIFICATION", "True").lower() == "true"
)

# Email settings
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "umldoomsday@gmail.com")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "twiq wczd mddm eznv")
# RECIPIENT_EMAIL line is now removed
