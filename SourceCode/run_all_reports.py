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
        msg.attach(MIMEText(body, "plain"))

        # Connect to the SMTP server and send the email
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()  # Secure the connection
            server.login(config.SENDER_EMAIL, config.SENDER_PASSWORD)
            server.send_message(msg)

        print("✅ Email notification sent successfully.")
    except Exception as e:
        print(f"❌ FAILED to send email. Error: {e}")


def run_reports():
    """Main function to orchestrate the generation of both reports."""
    start_time = time.time()

    print("==============================================")
    print("== STARTING AUTOMATED REPORT GENERATION ==")
    print("==============================================")

    success = False
    error_message = ""

    try:
        # --- 1. Run Summary Report ---
        print("\n[1/2] Generating Summary Report...")
        summary_reporter.generate_summary_report()
        print("✅ [1/2] Summary Report completed.")

        print("\n" + "-" * 45 + "\n")

        # --- 2. Run Ranking Report ---
        print("[2/2] Generating ASM Ranking Report...")
        ranking_reporter.generate_ranking_report()
        print("✅ [2/2] ASM Ranking Report completed.")

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

        # Send email notification based on the outcome
        if success:
            email_subject = "✅ Success: Report Generation Completed"
            email_body = (
                f"The automated report generation process has completed successfully.\n\n"
                f"Total execution time: {total_time:.2f} seconds.\n"
                f"Reports have been uploaded to Google Drive."
            )
        else:
            email_subject = "❌ Failure: Report Generation Encountered an Error"
            email_body = (
                f"The automated report generation process has failed.\n\n"
                f"Error details: {error_message}\n\n"
                f"Please check the execution logs for more information."
            )

        send_email(email_subject, email_body)


if __name__ == "__main__":
    run_reports()
