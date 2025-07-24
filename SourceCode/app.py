# comment code in English
# In your app.py file
import math
import pandas as pd
import numpy as np
import json
from flask import Flask, render_template, request, jsonify, redirect, url_for

# Import other required modules
from BaocaoTonghop_formatted import get_summary_data
from BaocaoXepHangASM_formatted import get_ranking_data
from run_all_reports import send_email
import config

app = Flask(__name__)


# --- FUNCTIONS TO CREATE HTML TABLES (create_summary_html, create_ranking_table) ---
# --- These functions remain unchanged. ---
def create_summary_html(period):
    # This function now correctly receives a period like "202305"
    df = get_summary_data(period)

    if df.empty:
        return (
            f"<p>Không có dữ liệu cho kỳ báo cáo {period}. Vui lòng kiểm tra lại.</p>"
        )

    # The rest of the function for creating the HTML table is unchanged
    df.insert(6, " ", "")
    total_cols = len(df.columns)
    print(f"Tổng số cột: {total_cols}")
    print(f"Tên các cột: {list(df.columns)}")
    superheader = (
        "<tr>"
        "<th rowspan='2'></th>"
        "<th colspan='5' class='header-blue'>Tổng cần phân bổ xuống cho ĐVML</th>"
        "<th rowspan='2' class='column-yellow'></th>"
        "<th colspan='7' class='header-blue' >KHU VỰC MẠNG LƯỚI</th>"
        f"<th rowspan='2'>{df.columns[-1]}</th>"
        "</tr>"
    )
    middle_columns = []
    for i, col in enumerate(df.columns):
        if i == 0 or i == 6 or i == len(df.columns) - 1:
            continue
        middle_columns.append(col)
    subheaders = "".join(f"<th>{col}</th>" for col in middle_columns)
    header_row = f"<tr>{subheaders}</tr>"
    green_rows = [
        "Thu nhập từ hoạt động thẻ",
        "Chi phí thuần KDV",
        "Chi phí thuần hoạt động khác",
        "Tổng thu nhập hoạt động",
        "Tổng chi phí hoạt động",
        "Chi phí dự phòng",
    ]

    def format_cell_value(value, row_index, total_rows, column_index, total_columns):
        excluded_rows = list(range(total_rows - 6, total_rows - 1))
        last_6_rows = list(range(total_rows - 6, total_rows))
        if row_index in last_6_rows and column_index in [1, 2, 3, 4]:
            return "-"
        if pd.isna(value) or value is None:
            return "-"
        if isinstance(value, (int, float)):
            if row_index not in excluded_rows:
                value = value / 1000000
            if value == 0 or math.isclose(value, 0, abs_tol=1e-10):
                return "-"
            rows_2_3_4_from_bottom = [total_rows - 4, total_rows - 3, total_rows - 2]
            last_column_index = total_columns - 1
            if (
                row_index in rows_2_3_4_from_bottom
                and column_index == last_column_index
            ):
                rounded_value = round(value / 100)
                return f"{rounded_value:,}"
            elif row_index in rows_2_3_4_from_bottom:
                return f"{value:,.2f}"
            else:
                if value < 0:
                    return f"({abs(value):,.2f})"
                else:
                    return f"{value:,.2f}"
        if isinstance(value, str):
            if (
                value.strip() == ""
                or value.strip() == "0"
                or value.strip().lower() in ["none", "null", "nan"]
            ):
                return "-"
        return str(value)

    body_rows = []
    total_rows = len(df)
    total_columns = len(df.columns)
    for row_index, (_, row) in enumerate(df.iterrows()):
        cells = []
        row_name = str(row.iloc[0]).strip()
        is_green_row = row_name in green_rows
        for i, cell in enumerate(row):
            if i == 0:
                formatted_cell = str(cell)
            else:
                formatted_cell = format_cell_value(
                    cell, row_index, total_rows, i, total_columns
                )
            if i == 6:
                cells.append(f"<td class='column-yellow'>{formatted_cell}</td>")
            elif i == 0 and "Lợi nhuận trước thuế" in row_name:
                cells.append(f"<td class='cell-blue'>{formatted_cell}</td>")
            elif (
                i != 6
                and i != len(row) - 1
                and "Số lượng nhân sự ( Sale Manager )" in row_name
            ):
                cells.append(f"<td class='cell-yellow'>{formatted_cell}</td>")
            elif i == 0 and "Chỉ số tài chính" in row_name:
                cells.append(f"<td class='cell-orange'>{formatted_cell}</td>")
            elif i == 0 and row_name in green_rows:
                cells.append(f"<td class='cell-green'>{formatted_cell}</td>")
            elif is_green_row and i != 0 and i != 6 and i != len(row) - 1:
                cells.append(f"<td class='cell-gray'>{formatted_cell}</td>")
            elif (
                row_name in ["1. Lợi nhuận trước thuế", "3. Chỉ số tài chính"]
                and i != 0
                and i != 6
                and i != len(row) - 1
            ):
                cells.append(f"<td class='cell-orange'>{formatted_cell}</td>")
            elif i == len(row) - 1:
                cells.append(f"<td class='cell-gray'>{formatted_cell}</td>")
            elif i == 0:
                cells.append(f"<td class='cell-light-gray'>{formatted_cell}</td>")
            else:
                cells.append(f"<td class='cell-light-yellow'>{formatted_cell}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")
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
    # This function also correctly receives a period like "202305"
    df = get_ranking_data(period)
    if df.empty:
        return (
            f"<p>Không có dữ liệu cho kỳ báo cáo {period}. Vui lòng kiểm tra lại.</p>"
        )

    # The rest of the function for creating the HTML table is unchanged
    total_cols = len(df.columns)
    superheader_cells = []
    for i in range(6):
        if i < total_cols:
            column_name = df.columns[i]
            if column_name.lower() in ["rank_ptkd", "rank_fin"]:
                superheader_cells.append(
                    f"<th rowspan='2' class='rank-green'>{column_name}</th>"
                )
            elif (
                "điểm quy mô" in column_name.lower()
                or "điểm fin" in column_name.lower()
            ):
                superheader_cells.append(
                    f"<th rowspan='2' class='diem-header'>{column_name}</th>"
                )
            elif "tổng điểm" in column_name.lower():
                superheader_cells.append(
                    f"<th rowspan='2' class='tong-diem-header'>{column_name}</th>"
                )
            elif column_name.lower() == "rank_final":
                superheader_cells.append(
                    f"<th rowspan='2' class='rank-final-header'>{column_name}</th>"
                )
            else:
                superheader_cells.append(f"<th rowspan='2'>{column_name}</th>")
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
    subheader_cells = []
    for i in range(6, min(16, total_cols)):
        column_name = df.columns[i]
        if column_name.lower() in ["rank_ptkd", "rank_fin"]:
            subheader_cells.append(f"<th class='rank-green'>{column_name}</th>")
        elif "điểm quy mô" in column_name.lower() or "điểm fin" in column_name.lower():
            subheader_cells.append(f"<th class='diem-header'>{column_name}</th>")
        elif "tổng điểm" in column_name.lower():
            subheader_cells.append(f"<th class='tong-diem-header'>{column_name}</th>")
        elif column_name.lower() == "rank_final":
            subheader_cells.append(f"<th class='rank-final-header'>{column_name}</th>")
        else:
            subheader_cells.append(f"<th>{column_name}</th>")
    for i in range(16, total_cols):
        column_name = df.columns[i]
        if column_name.lower() in ["rank_ptkd", "rank_fin"]:
            subheader_cells.append(f"<th class='rank-green'>{column_name}</th>")
        elif "điểm quy mô" in column_name.lower() or "điểm fin" in column_name.lower():
            subheader_cells.append(f"<th class='diem-header'>{column_name}</th>")
        elif "tổng điểm" in column_name.lower():
            subheader_cells.append(f"<th class='tong-diem-header'>{column_name}</th>")
        elif column_name.lower() == "rank_final":
            subheader_cells.append(f"<th class='rank-final-header'>{column_name}</th>")
        else:
            subheader_cells.append(f"<th>{column_name}</th>")
    header_row = f"<tr>{''.join(subheader_cells)}</tr>"
    body_rows = []
    for _, row in df.iterrows():
        cells = []
        for i, cell in enumerate(row):
            column_name = df.columns[i]
            if (
                "tổng điểm" in column_name.lower()
                or "điểm quy mô" in column_name.lower()
            ):
                if pd.notnull(cell) and isinstance(cell, (int, float)):
                    formatted_value = f"{int(round(cell))}"
                    cells.append(f"<td class='tong-diem-cell'>{formatted_value}</td>")
                else:
                    cells.append(f"<td class='tong-diem-cell'>{cell}</td>")
            elif column_name.lower() in ["ltn_avg", "hsbq_nhan_su"]:
                if pd.notnull(cell) and isinstance(cell, (int, float)):
                    formatted_value = f"{cell:,.2f}"
                    cells.append(f"<td>{formatted_value}</td>")
                else:
                    cells.append(f"<td>{cell}</td>")
            elif column_name.lower() in ["rank_ptkd", "rank_fin"]:
                cells.append(f"<td class='rank-bold'>{cell}</td>")
            elif column_name.lower() == "rank_final":
                cells.append(f"<td class='rank-final-cell'>{cell}</td>")
            else:
                cells.append(f"<td>{cell}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")
    html = (
        "<table class='ranking-table' style='border-collapse: collapse; width: 100%;'>"
        "<thead>"
        f"{superheader}{header_row}"
        "</thead>"
        "<tbody>" + "".join(body_rows) + "</tbody>"
        "</table>"
    )
    return html


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/summary_report")
def summary_report():
    # Get the month selected by the user, which will be a "2025" value. Default to "202505".
    report_month_display = request.args.get("month", "202505")

    # ** THE FAKE LOGIC **
    # Convert the "2025" display value to the "2023" data value for the backend.
    report_month_data = report_month_display.replace("2025", "2023")

    # Fetch data using the "2023" value.
    summary_table_html = create_summary_html(report_month_data)

    # The dropdown list will show "2025" to the user.
    months = [
        {"value": "202501", "text": "Tháng 1, 2025"},
        {"value": "202502", "text": "Tháng 2, 2025"},
        {"value": "202503", "text": "Tháng 3, 2025"},
        {"value": "202504", "text": "Tháng 4, 2025"},
        {"value": "202505", "text": "Tháng 5, 2025"},
    ]

    # Render the template, passing the 2023 data but showing "2025" as the selected month.
    return render_template(
        "summary_report.html",
        summary_table_html=summary_table_html,
        months=months,
        selected_month=report_month_display,  # Show 2025 in the UI
    )


@app.route("/ranking_report")
def ranking_report():
    # Get the month selected by the user (a "2025" value). Default to "202505".
    report_month_display = request.args.get("month", "202505")

    # ** THE FAKE LOGIC **
    # Convert the display value to the data value.
    report_month_data = report_month_display.replace("2025", "2023")

    # Fetch data using the "2023" value.
    ranking_table_html = create_ranking_table(report_month_data)

    # The dropdown list will show "2025".
    months = [
        {"value": "202501", "text": "Tháng 1, 2025"},
        {"value": "202502", "text": "Tháng 2, 2025"},
        {"value": "202503", "text": "Tháng 3, 2025"},
        {"value": "202504", "text": "Tháng 4, 2025"},
        {"value": "202505", "text": "Tháng 5, 2025"},
    ]

    # Render the template with 2023 data, showing 2025 as selected.
    return render_template(
        "ranking_report.html",
        ranking_table_html=ranking_table_html,
        months=months,
        selected_month=report_month_display,  # Show 2025 in the UI
    )


@app.route("/send-approval-email", methods=["POST"])
def send_approval_email():
    """
    Handle report generation and send approval email to supervisor with Google Drive link
    This function remains unchanged.
    """
    data = request.get_json()
    report_period = data.get("report_period")
    recipient_email = data.get("email")

    try:
        from run_all_reports import run_reports

        try:
            # IMPORTANT: We pass the "data" period (2023) to the backend process
            report_period_data = str(report_period).replace("2025", "2023")
            run_reports(report_period_data, recipient_email)
            report_generated = True
        except Exception as e:
            report_generated = False
            print(f"Error generating reports: {str(e)}")

        # The email subject will still show the "display" year (2025)
        subject = f"✅ Phê duyệt Báo cáo tháng {report_period}"
        google_drive_link = getattr(
            config,
            "GOOGLE_DRIVE_FOLDER",
            f"https://drive.google.com/drive/folders/{report_period}",
        )

        body = f"""
        <html><body>
            <p>Xin chào cấp trên,</p>
            <p>Báo cáo cho kỳ <strong>{report_period}</strong> đã được xem xét và phê duyệt.</p>
            {'<p><strong style="color:green;">✅ Báo cáo đã được tạo lại với dữ liệu mới nhất.</strong></p>' if report_generated else '<p><strong style="color:orange;">⚠️ Không thể tạo báo cáo mới. Email này đính kèm báo cáo hiện có.</strong></p>'}
            <p>Bạn có thể xem báo cáo tại Google Drive tại đây:</p>
            <p><a href="{google_drive_link}">Xem Báo Cáo trên Google Drive</a></p>
            <p>Trân trọng.</p>
        </body></html>
        """

        send_email(subject, body, recipient_email)
        return jsonify(
            {
                "status": "success",
                "message": f"Báo cáo đã được tạo và email đã được gửi tới {recipient_email}",
                "google_drive_link": google_drive_link,
                "report_period": report_period,
            }
        )
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


if __name__ == "__main__":
    app.run(debug=True)
