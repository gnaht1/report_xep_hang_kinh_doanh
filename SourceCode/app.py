# app.py

# Import necessary libraries
import math
import pandas as pd
import numpy as np
import json
from flask import Flask, render_template, request, jsonify, redirect, url_for

# Import custom modules for data fetching and email functionality
from BaocaoTonghop_formatted import get_summary_data
from BaocaoXepHangASM_formatted import get_ranking_data
from run_all_reports import send_email
import config

# Initialize the Flask application
app = Flask(__name__)


# --- HTML TABLE CREATION FUNCTIONS ---


def create_summary_html(period):
    """
    Fetches summary data for a given period and generates an HTML table.

    Args:
        period (str): The reporting period in "YYYYMM" format.

    Returns:
        str: An HTML string representing the summary table.
    """
    # Fetch the summary data for the specified period
    df = get_summary_data(period)

    # If the DataFrame is empty, return a message indicating no data
    if df.empty:
        return f"<p>No data available for the reporting period {period}. Please check again.</p>"

    # --- Table Structure Setup ---
    # Insert an empty column at index 6 to act as a visual separator
    df.insert(6, " ", "")
    total_cols = len(df.columns)

    # Create the top-level header (superheader) spanning multiple columns
    superheader = (
        "<tr>"
        "<th rowspan='2' style='text-align: left;'></th>"  # First column header is empty and left-aligned
        "<th colspan='5' class='header-blue'>Tổng cần phân bổ xuống cho ĐVML</th>"
        "<th rowspan='2' class='column-yellow'></th>"  # Separator column
        "<th colspan='7' class='header-blue' >KHU VỰC MẠNG LƯỚI</th>"
        f"<th rowspan='2'>{df.columns[-1]}</th>"  # Last column header
        "</tr>"
    )

    # Create the second-level header (subheader) with individual column titles
    middle_columns = []
    for i, col in enumerate(df.columns):
        # Skip columns that are already handled by rowspan in the superheader
        if i in [0, 6, len(df.columns) - 1]:
            continue
        middle_columns.append(col)

    subheaders = "".join(f"<th>{col}</th>" for col in middle_columns)
    header_row = f"<tr>{subheaders}</tr>"

    # Define a list of row names that require special green styling
    green_rows = [
        "1. Thu nhập từ hoạt động thẻ",
        "2. Chi phí thuần KDV",
        "3. Chi phí thuần hoạt động khác",
        "4. Tổng thu nhập hoạt động",
        "5. Tổng chi phí hoạt động",
        "6. Chi phí dự phòng",
    ]

    def format_cell_value(value, row_index, total_rows, column_index, total_columns):
        """Formats the value of a single cell based on its position and content."""
        excluded_rows = list(range(total_rows - 6, total_rows - 1))
        last_6_rows = list(range(total_rows - 6, total_rows))

        # For the last 6 rows, set specific columns (1-4) to "-"
        if row_index in last_6_rows and column_index in [1, 2, 3, 4]:
            return "-"

        # Handle null or NaN values
        if pd.isna(value) or value is None:
            return "-"

        # Process numeric values
        if isinstance(value, (int, float)):
            # Divide by 1,000,000 for most rows, except those specified
            if row_index not in excluded_rows:
                value = value / 1000000

            # Return "-" for zero values to keep the table clean
            if value == 0 or math.isclose(value, 0, abs_tol=1e-10):
                return "-"

            # Special formatting for rows 2, 3, 4 from the bottom
            rows_2_3_4_from_bottom = [total_rows - 4, total_rows - 3, total_rows - 2]
            last_column_index = total_columns - 1

            # Round to integer for the last column of these specific rows
            if (
                row_index in rows_2_3_4_from_bottom
                and column_index == last_column_index
            ):
                rounded_value = round(value / 100)
                return f"{rounded_value:,}"
            # Keep 2 decimal places for other cells in these rows
            elif row_index in rows_2_3_4_from_bottom:
                return f"{value:,.2f}"
            # Format negative numbers with parentheses for all other numeric cells
            else:
                if value < 0:
                    return f"({abs(value):,.2f})"
                else:
                    return f"{value:,.2f}"

        # Handle string values, replacing common null-like strings with "-"
        if isinstance(value, str):
            if value.strip().lower() in ["", "0", "none", "null", "nan"]:
                return "-"
        return str(value)

    # --- Build Table Body ---
    body_rows = []
    total_rows = len(df)

    for row_index, (_, row) in enumerate(df.iterrows()):
        cells = []
        row_name = str(row.iloc[0]).strip()
        is_green_row = row_name in green_rows

        for i, cell in enumerate(row):
            # Format the cell's value unless it's the first column (row header)
            formatted_cell = (
                str(cell)
                if i == 0
                else format_cell_value(cell, row_index, total_rows, i, total_cols)
            )

            # --- Apply Styling and Alignment ---

            # Style the separator column
            if i == 6:
                cells.append(f"<td class='column-yellow'>{formatted_cell}</td>")

            # Handle the first column (i == 0) which requires special logic for both alignment and color
            elif i == 0:
                # **FIX START**: Added a specific check for "Số lượng nhân sự" row inside this block
                if "II. Số lượng nhân sự ( Sale Manager )" in row_name:
                    cells.append(
                        f"<td class='cell-yellow' style='text-align: left;'>{formatted_cell}</td>"
                    )
                # **FIX END**
                elif "I. Lợi nhuận trước thuế" in row_name:
                    cells.append(
                        f"<td class='cell-blue' style='text-align: left;'>{formatted_cell}</td>"
                    )
                elif "III. Chỉ số tài chính" in row_name:
                    cells.append(
                        f"<td class='cell-orange' style='text-align: left;'>{formatted_cell}</td>"
                    )
                elif row_name in green_rows:
                    cells.append(
                        f"<td class='cell-green' style='text-align: left;'>{formatted_cell}</td>"
                    )
                else:
                    # Default alignment and color for all other first-column cells
                    cells.append(
                        f"<td class='cell-light-gray' style='text-align: left;'>{formatted_cell}</td>"
                    )

            # Style other data cells based on row name and styling flags
            elif (
                "II. Số lượng nhân sự ( Sale Manager )" in row_name
                and i != 6
                and i != len(row) - 1
            ):
                cells.append(f"<td class='cell-yellow'>{formatted_cell}</td>")
            elif is_green_row and i != 6 and i != len(row) - 1:
                cells.append(f"<td class='cell-gray'>{formatted_cell}</td>")
            elif (
                row_name in ["I. Lợi nhuận trước thuế", "III. Chỉ số tài chính"]
                and i != 6
                and i != len(row) - 1
            ):
                cells.append(f"<td class='cell-orange'>{formatted_cell}</td>")
            elif i == len(row) - 1:
                cells.append(f"<td class='cell-gray'>{formatted_cell}</td>")
            else:
                cells.append(f"<td class='cell-light-yellow'>{formatted_cell}</td>")

        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    # Construct the final HTML table string
    html = (
        "<table class='summary-table'>"
        "<thead>"
        f"{superheader}{header_row}"
        "</thead>"
        "<tbody>" + "".join(body_rows) + "</tbody>"
        "</table>"
    )
    return html


def create_ranking_table(period):
    """
    Fetches ranking data for a given period and generates an HTML table.

    Args:
        period (str): The reporting period in "YYYYMM" format.

    Returns:
        str: An HTML string representing the ranking table.
    """
    df = get_ranking_data(period)
    if df.empty:
        return f"<p>No data available for the reporting period {period}. Please check again.</p>"

    total_cols = len(df.columns)

    # --- Superheader and Subheader Creation ---
    superheader_cells = []
    # First loop for columns that have a rowspan of 2
    for i in range(total_cols):
        if i >= 6:
            continue  # Only process the first 6 columns here

        column_name = df.columns[i]
        if i == 0:
            superheader_cells.append(f"<th rowspan='2'>Tháng Báo Cáo</th>")
        elif i == 1:
            superheader_cells.append(f"<th rowspan='2'>Mã Khu Vực</th>")
        elif i == 2:
            superheader_cells.append(f"<th rowspan='2'>Tên Khu Vực</th>")
        elif i == 5:
            superheader_cells.append(
                f"<th rowspan='2' class='rank-final-header'>Xếp Hạng Cuối</th>"
            )
        elif column_name.lower() in ["rank_ptkd", "rank_fin"]:
            superheader_cells.append(
                f"<th rowspan='2' class='rank-green'>{column_name}</th>"
            )
        elif "điểm quy mô" in column_name.lower() or "điểm fin" in column_name.lower():
            superheader_cells.append(
                f"<th rowspan='2' class='diem-header'>{column_name}</th>"
            )
        elif "tổng điểm" in column_name.lower():
            superheader_cells.append(
                f"<th rowspan='2' class='tong-diem-header'>{column_name}</th>"
            )
        else:
            superheader_cells.append(f"<th rowspan='2'>{column_name}</th>")

    # Add merged superheader cells for 'QUY MÔ' and 'TÀI CHÍNH' sections
    if total_cols > 6:
        quy_mo_colspan = min(10, total_cols - 6)
        superheader_cells.append(
            f"<th colspan='{quy_mo_colspan}' style='background-color: #4472C4; color: white; text-align: center;'>QUY MÔ</th>"
        )
    if total_cols > 16:
        tai_chinh_colspan = total_cols - 16
        superheader_cells.append(
            f"<th colspan='{tai_chinh_colspan}' style='background-color: #FF6600; color: white; text-align: center;'>TÀI CHÍNH</th>"
        )
    superheader = f"<tr>{''.join(superheader_cells)}</tr>"

    # Create the subheader row for columns under the merged sections
    subheader_cells = []
    # Loop for 'QUY MÔ' subheaders
    for i in range(6, min(16, total_cols)):
        column_name = df.columns[i]
        # Using custom, more descriptive names for some columns
        if i == 6:
            subheader_cells.append(f"<th>LTN TB</th>")
        elif i == 7:
            subheader_cells.append(f"<th>Xếp Hạng LTN TB</th>")
        elif i == 8:
            subheader_cells.append(f"<th>PSDN TB</th>")
        elif i == 9:
            subheader_cells.append(f"<th>Xếp Hạng PSDN TB</th>")
        elif i == 10:
            subheader_cells.append(f"<th>Approval Rate TB</th>")
        elif i == 11:
            subheader_cells.append(f"<th>Xếp Hạng Approval Rate TB</th>")
        elif i == 12:
            subheader_cells.append(f"<th>NPL Trước Write Off Luỹ Kế</th>")
        elif i == 13:
            subheader_cells.append(f"<th>Xếp Hạng NPL Trước Write Off Luỹ Kế</th>")
        elif i == 15:
            subheader_cells.append(
                "<th class='rank-green'>Xếp Hạng Phát Triển Kinh Doanh</th>"
            )
        # Apply styling based on column name
        elif "điểm quy mô" in column_name.lower() or "điểm fin" in column_name.lower():
            subheader_cells.append(f"<th class='diem-header'>{column_name}</th>")
        else:
            subheader_cells.append(f"<th>{column_name}</th>")

    # Loop for 'TÀI CHÍNH' subheaders
    for i in range(16, total_cols):
        column_name = df.columns[i]
        if i == 17:
            subheader_cells.append(f"<th>Xếp Hạng CIR</th>")
        elif i == 19:
            subheader_cells.append(f"<th>Xếp Hạng Margin</th>")
        elif i == 20:
            subheader_cells.append(f"<th>Hiệu Suất Vốn</th>")
        elif i == 21:
            subheader_cells.append(f"<th>Xếp Hạng Hiệu Suất Vốn</th>")
        elif i == 22:
            subheader_cells.append(f"<th>Hiệu Suất Bình Quân Nhân Sự</th>")
        elif i == 23:
            subheader_cells.append(f"<th>Xếp Hạng Hiệu Suất Bình Quân Nhân Sự</th>")
        elif i == 25:
            subheader_cells.append(f"<th class='rank-green'>Xếp Hạng Điểm FIN</th>")
        else:
            subheader_cells.append(f"<th>{column_name}</th>")
    header_row = f"<tr>{''.join(subheader_cells)}</tr>"

    # --- Build Table Body ---
    body_rows = []
    for _, row in df.iterrows():
        cells = []
        for i, cell in enumerate(row):
            column_name = df.columns[i]

            # Format the first column from 'YYYYMM' to 'MM/YYYY'
            if i == 0:
                if pd.notnull(cell) and len(str(cell)) == 6:
                    formatted_value = f"{str(cell)[4:]}/{str(cell)[:4]}"
                else:
                    formatted_value = cell  # Fallback for unexpected format
                cells.append(f"<td>{formatted_value}</td>")
            # Format numeric cells based on column name
            elif (
                "tổng điểm" in column_name.lower()
                or "điểm quy mô" in column_name.lower()
            ):
                formatted_value = f"{int(round(cell))}" if pd.notnull(cell) else cell
                cells.append(f"<td class='tong-diem-cell'>{formatted_value}</td>")
            elif column_name.lower() in ["ltn_avg", "hsbq_nhan_su"]:
                formatted_value = f"{cell:,.2f}" if pd.notnull(cell) else cell
                cells.append(f"<td>{formatted_value}</td>")
            elif column_name.lower() in [
                "approval_rate_avg",
                "npl_truoc_wo_luy_ke",
                "cir",
                "margin",
                "hs_von",
            ]:
                formatted_value = f"{cell:,.8f}" if pd.notnull(cell) else cell
                cells.append(f"<td>{formatted_value}</td>")
            # Apply bold styling to rank columns
            elif column_name.lower() in ["rank_ptkd", "rank_fin"]:
                cells.append(f"<td class='rank-bold'>{cell}</td>")
            elif column_name.lower() == "rank_final":
                cells.append(f"<td class='rank-final-cell'>{cell}</td>")
            else:
                cells.append(f"<td>{cell}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    # Construct the final HTML table
    html = (
        "<table class='ranking-table'>"
        "<thead>"
        f"{superheader}{header_row}"
        "</thead>"
        "<tbody>" + "".join(body_rows) + "</tbody>"
        "</table>"
    )
    return html


# --- FLASK ROUTES ---


@app.route("/")
def home():
    """Renders the home page."""
    return render_template("home.html")


@app.route("/summary_report")
def summary_report():
    """Renders the summary report page for a selected month."""
    # Get the selected month from URL parameters, default to May 2025 for display
    report_month_display = request.args.get("month", "202505")
    # The backend data uses '2023', so we replace '2025' for the data fetching part
    report_month_data = report_month_display.replace("2025", "2023")

    # Generate the HTML table
    summary_table_html = create_summary_html(report_month_data)

    # Define the list of months for the dropdown menu
    months = [
        {"value": "202501", "text": "Tháng 1, 2025"},
        {"value": "202502", "text": "Tháng 2, 2025"},
        {"value": "202503", "text": "Tháng 3, 2025"},
        {"value": "202504", "text": "Tháng 4, 2025"},
        {"value": "202505", "text": "Tháng 5, 2025"},
    ]

    # Render the template with the necessary data
    return render_template(
        "summary_report.html",
        summary_table_html=summary_table_html,
        months=months,
        selected_month=report_month_display,
    )


@app.route("/ranking_report")
def ranking_report():
    """Renders the ranking report page for a selected month."""
    report_month_display = request.args.get("month", "202505")
    report_month_data = report_month_display.replace("2025", "2023")

    ranking_table_html = create_ranking_table(report_month_data)

    months = [
        {"value": "202501", "text": "Tháng 1, 2025"},
        {"value": "202502", "text": "Tháng 2, 2025"},
        {"value": "202503", "text": "Tháng 3, 2025"},
        {"value": "202504", "text": "Tháng 4, 2025"},
        {"value": "202505", "text": "Tháng 5, 2025"},
    ]

    return render_template(
        "ranking_report.html",
        ranking_table_html=ranking_table_html,
        months=months,
        selected_month=report_month_display,
    )


@app.route("/send-approval-email", methods=["POST"])
def send_approval_email():
    """API endpoint to handle sending the report email."""
    data = request.get_json()
    report_period = data.get("report_period")
    recipient_email = data.get("email")

    try:
        # Import the report generation function
        from run_all_reports import run_reports

        # Try to regenerate the report files with the latest data
        try:
            report_period_data = str(report_period).replace("2025", "2023")
            run_reports(report_period_data, recipient_email)
            report_generated = True
        except Exception as e:
            report_generated = False
            print(f"Error generating reports: {str(e)}")

        # Prepare email subject and body
        subject = f"✅ Phê duyệt Báo cáo tháng {report_period}"
        google_drive_link = getattr(
            config,
            "GOOGLE_DRIVE_FOLDER",
            f"https://drive.google.com/drive/folders/{report_period}",
        )

        body = f"""
        <html><body>
            <p>Xin chào,</p>
            <p>Báo cáo cho kỳ <strong>{report_period}</strong> đã được xem xét và gửi đi.</p>
            {'<p><strong style="color:green;">✅ Báo cáo đã được tạo lại với dữ liệu mới nhất.</strong></p>' if report_generated else '<p><strong style="color:orange;">⚠️ Không thể tạo báo cáo mới. Email này đính kèm báo cáo hiện có.</strong></p>'}
            <p>Bạn có thể xem báo cáo tại Google Drive tại đây:</p>
            <p><a href="{google_drive_link}">Xem Báo Cáo trên Google Drive</a></p>
            <p>Trân trọng.</p>
        </body></html>
        """

        # Send the email
        send_email(subject, body, recipient_email)

        # Return a success response
        return jsonify(
            {
                "status": "success",
                "message": f"Báo cáo đã được tạo và email đã được gửi tới {recipient_email}",
                "google_drive_link": google_drive_link,
                "report_period": report_period,
            }
        )
    except Exception as e:
        # Return an error response if anything fails
        return jsonify({"status": "error", "message": str(e)})


# Run the Flask app in debug mode if the script is executed directly
if __name__ == "__main__":
    app.run(debug=True)
