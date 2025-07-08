# -*- coding: utf-8 -*-
import time
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Try to import the necessary modules
try:
    import config
    import BaocaoTonghop_formatted as summary_reporter
    import BaocaoXepHangASM_formatted as ranking_reporter
except ImportError as e:
    print(f"ERROR: Could not import a required module. {e}")
    sys.exit(1)


def send_email(subject, body):
    """Sends an email notification using settings from config.py."""
    if not config.ENABLE_EMAIL_NOTIFICATION:
        print("Email notifications are disabled in the config file.")
        return

    print("Attempting to send email notification...")
    try:
        # Create the email message
        msg = MIMEMultipart()
        msg["From"] = config.SENDER_EMAIL
        msg["To"] = config.RECIPIENT_EMAIL
        msg["Subject"] = subject

        # CHANGE 1: Send the email as HTML instead of plain text
        # This is what makes the link clickable.
        msg.attach(MIMEText(body, "html"))

        # Connect to the SMTP server and send the email
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SENDER_EMAIL, config.SENDER_PASSWORD)
            server.send_message(msg)

        print("✅ Email notification sent successfully.")
    except Exception as e:
        print(f"❌ FAILED to send email. Error: {e}")


def run_reports(report_period):
    """Main function to orchestrate the generation of both reports for a given period."""
    start_time = time.time()

    print(f"==============================================")
    print(f"== STARTING REPORT GENERATION FOR PERIOD: {report_period} ==")
    print(f"==============================================")

    success = False
    error_message = ""

    try:
        summary_reporter.generate_summary_report(report_period)
        ranking_reporter.generate_ranking_report(report_period)
        success = True
    except Exception as e:
        error_message = str(e)
        print(f"\n[CRITICAL ERROR] An unexpected error occurred: {error_message}")

    finally:
        end_time = time.time()
        total_time = end_time - start_time

        print("\n==============================================")
        print("== REPORT GENERATION PROCESS FINISHED ==")
        print(f"== Total execution time: {total_time:.2f} seconds ==")
        print("==============================================")

        # CHANGE 2: Build the Drive URL and add it to the email body
        folder_id = config.GOOGLE_DRIVE_FOLDER_ID
        drive_folder_url = None
        if "YOUR_GOOGLE_DRIVE_FOLDER_ID_HERE" not in folder_id and folder_id:
            drive_folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

        if success:
            email_subject = f"✅ Success: Report for {report_period} Completed"
            email_body = f"""
            <html>
            <body>
                <p>The automated report generation process for period <strong>{report_period}</strong> has completed successfully.</p>
                <p>Total execution time: {total_time:.2f} seconds.</p>
            """
            if drive_folder_url:
                email_body += f'<p>The generated reports have been uploaded to Google Drive. You can view them here:</p><p><a href="{drive_folder_url}">View Google Drive Folder</a></p>'
            email_body += "</body></html>"
        else:
            email_subject = f"❌ Failure: Report for {report_period} Failed"
            email_body = f"""
            <html>
            <body>
                <p>The automated report generation process for period <strong>{report_period}</strong> has failed.</p>
                <p><strong>Error details:</strong> {error_message}</p>
                <p>Please check the execution logs for more information.</p>
            </body>
            </html>
            """

        send_email(email_subject, email_body)


# This block is only used if you run this script directly
# It is not used when running from the Flask web app
if __name__ == "__main__":
    # Example of how to run directly for testing purposes
    test_period = "202305"
    run_reports(test_period)
