from flask import Flask, render_template, request, redirect, url_for, flash
import threading
import config
from run_all_reports import run_reports

app = Flask(__name__)
app.secret_key = "a_very_secret_key_for_flashing"


@app.route("/")
def index():
    folder_id = config.GOOGLE_DRIVE_FOLDER_ID
    drive_folder_url = None
    if "YOUR_GOOGLE_DRIVE_FOLDER_ID_HERE" not in folder_id and folder_id:
        drive_folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
    return render_template("index.html", drive_folder_url=drive_folder_url)


@app.route("/trigger", methods=["POST"])
def trigger_report():
    year = request.form.get("year")
    month = request.form.get("month")
    # Get the user's email from the form
    recipient_email = request.form.get("email")

    if not all([year, month, recipient_email]):
        flash("Please fill out all fields.", "error")
        return redirect(url_for("index"))

    report_period = f"{year}{int(month):02d}"

    print(
        f"Web request received for period {report_period}. Notifying {recipient_email}"
    )

    # Pass the user's email to the background thread
    report_thread = threading.Thread(
        target=run_reports, args=(report_period, recipient_email)
    )
    report_thread.start()

    flash(
        f"✅ The report for period {report_period} has started. A notification will be sent to {recipient_email} upon completion.",
        "success",
    )
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
