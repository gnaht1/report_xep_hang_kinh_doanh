from flask import Flask, render_template, request, redirect, url_for, flash
import threading

# Import the runner function and the config file
from run_all_reports import run_reports
import config

app = Flask(__name__)
app.secret_key = "a_very_secret_key_for_flashing"


@app.route("/")
def index():
    """Renders the main page with the date selection form."""
    # Construct the Google Drive folder URL from the config file
    folder_id = config.GOOGLE_DRIVE_FOLDER_ID
    # Ensure the ID is not the default placeholder before creating a link
    if "YOUR_GOOGLE_DRIVE_FOLDER_ID_HERE" in folder_id:
        drive_folder_url = None  # Don't show a link if the ID is not set
    else:
        drive_folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

    return render_template("index.html", drive_folder_url=drive_folder_url)


@app.route("/trigger", methods=["POST"])
def trigger_report():
    """Handles the form submission and starts the report generation."""
    year = request.form.get("year")
    month = request.form.get("month")

    if not year or not month:
        flash("Please select both a month and a year.", "error")
        return redirect(url_for("index"))

    report_period = f"{year}{int(month):02d}"

    print(
        f"Web request received. Starting report generation for period: {report_period}"
    )

    # Run the report generation in a separate thread
    report_thread = threading.Thread(target=run_reports, args=(report_period,))
    report_thread.start()

    # Update the flash message to be more informative
    flash(
        f"The report generation for period {report_period} has started. You will receive an email upon completion.",
        "success",
    )
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
