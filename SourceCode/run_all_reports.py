import time
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import config
import BaocaoTonghop_formatted as summary_reporter
import BaocaoXepHangASM_formatted as ranking_reporter


# The send_email function now accepts a 'recipient' parameter
def send_email(subject, body, recipient):
    if not config.ENABLE_EMAIL_NOTIFICATION:
        print("Email notifications are disabled.")
        return

    print(f"Attempting to send email notification to {recipient}...")
    try:
        msg = MIMEMultipart()
        msg["From"] = config.SENDER_EMAIL
        # Use the recipient passed to the function
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SENDER_EMAIL, config.SENDER_PASSWORD)
            server.send_message(msg)

        print(f"✅ Email notification sent successfully to {recipient}.")
    except Exception as e:
        print(f"❌ FAILED to send email. Error: {e}")


# The run_reports function now accepts a 'recipient_email' parameter
def run_reports(report_period, recipient_email):
    start_time = time.time()
    print(f"== STARTING REPORT GENERATION FOR PERIOD: {report_period} ==")

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
        print(f"== REPORT GENERATION PROCESS FINISHED IN {total_time:.2f} SECONDS ==")

        folder_id = config.GOOGLE_DRIVE_FOLDER_ID
        drive_folder_url = None
        if "YOUR_GOOGLE_DRIVE_FOLDER_ID_HERE" not in folder_id and folder_id:
            drive_folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

        display_period = str(report_period).replace("2023", "2025")

        if success:
            email_subject = f"✅ Phê duyệt Báo cáo tháng {display_period}"
            email_body = f"""
            <html><body>
                <p>Xin chào,</p>
                <p>Báo cáo cho kỳ <strong>{display_period}</strong> đã được xem xét và gửi đi.</p>
                <p><strong style="color:green;">✅ Báo cáo đã được tạo lại với dữ liệu mới nhất.</strong></p>
                <p>Tổng thời gian thực thi: {total_time:.2f} giây.</p>
                {'<p>Bạn có thể xem báo cáo tại đây: <a href="' + drive_folder_url + '">Xem thư mục Google Drive</a></p>' if drive_folder_url else ""}
                <p>Trân trọng.</p>
            </body></html>"""
        else:
            email_subject = f"❌ Failure: Report for {display_period} Failed"
            email_body = f"""
            <html><body>
                <p>The report generation for period <strong>{display_period}</strong> has failed.</p>
                <p><strong>Error details:</strong> {error_message}</p>
            </body></html>"""

        # Pass the recipient_email to the send_email function
        send_email(email_subject, email_body, recipient_email)


if __name__ == "__main__":
    run_reports("202302", "umldoomsday@gmail.com")
