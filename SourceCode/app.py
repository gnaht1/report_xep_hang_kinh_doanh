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
def create_summary_html(period):
    # This function now correctly receives a period like "202305"
    df = get_summary_data(period)

    if df.empty:
        return (
            f"<p>Không có dữ liệu cho kỳ báo cáo {period}. Vui lòng kiểm tra lại.</p>"
        )

    # Insert empty column and prepare table structure
    df.insert(6, " ", "")
    total_cols = len(df.columns)
    print(f"Tổng số cột: {total_cols}")
    print(f"Tên các cột: {list(df.columns)}")

    # Create superheader with first column left-aligned
    superheader = (
        "<tr>"
        "<th rowspan='2' style='text-align: left;'></th>"
        "<th colspan='5' class='header-blue'>Tổng cần phân bổ xuống cho ĐVML</th>"
        "<th rowspan='2' class='column-yellow'></th>"
        "<th colspan='7' class='header-blue' >KHU VỰC MẠNG LƯỚI</th>"
        f"<th rowspan='2'>{df.columns[-1]}</th>"
        "</tr>"
    )

    # Create middle columns for subheader
    middle_columns = []
    for i, col in enumerate(df.columns):
        if i == 0 or i == 6 or i == len(df.columns) - 1:
            continue
        middle_columns.append(col)
    subheaders = "".join(f"<th>{col}</th>" for col in middle_columns)
    header_row = f"<tr>{subheaders}</tr>"

    # Define green rows for special styling
    green_rows = [
        "1. Thu nhập từ hoạt động thẻ",
        "2. Chi phí thuần KDV",
        "3. Chi phí thuần hoạt động khác",
        "4. Tổng thu nhập hoạt động",
        "5. Tổng chi phí hoạt động",
        "6. Chi phí dự phòng",
    ]

    def format_cell_value(value, row_index, total_rows, column_index, total_columns):
        # Exclude certain rows from division by 1,000,000
        excluded_rows = list(range(total_rows - 6, total_rows - 1))
        last_6_rows = list(range(total_rows - 6, total_rows))

        # Set null values for specific cells in last 6 rows, columns 2-5
        if row_index in last_6_rows and column_index in [1, 2, 3, 4]:
            return "-"

        # Handle null values
        if pd.isna(value) or value is None:
            return "-"

        # Process numeric values
        if isinstance(value, (int, float)):
            # Divide by 1,000,000 for most rows except excluded ones
            if row_index not in excluded_rows:
                value = value / 1000000

            # Return dash for zero values
            if value == 0 or math.isclose(value, 0, abs_tol=1e-10):
                return "-"

            # Special formatting for rows 2, 3, 4 from bottom
            rows_2_3_4_from_bottom = [total_rows - 4, total_rows - 3, total_rows - 2]
            last_column_index = total_columns - 1

            # Round to integer for last column of specific rows
            if (
                row_index in rows_2_3_4_from_bottom
                and column_index == last_column_index
            ):
                rounded_value = round(value / 100)
                return f"{rounded_value:,}"
            # Keep 2 decimal places for other cells in those rows
            elif row_index in rows_2_3_4_from_bottom:
                return f"{value:,.2f}"
            # Format negative values with parentheses for other rows
            else:
                if value < 0:
                    return f"({abs(value):,.2f})"
                else:
                    return f"{value:,.2f}"

        # Handle string values
        if isinstance(value, str):
            if (
                value.strip() == ""
                or value.strip() == "0"
                or value.strip().lower() in ["none", "null", "nan"]
            ):
                return "-"
        return str(value)

    # Build table body rows
    body_rows = []
    total_rows = len(df)
    total_columns = len(df.columns)

    for row_index, (_, row) in enumerate(df.iterrows()):
        cells = []
        row_name = str(row.iloc[0]).strip()
        is_green_row = row_name in green_rows

        for i, cell in enumerate(row):
            # Format cell value based on column position
            if i == 0:
                formatted_cell = str(cell)
            else:
                formatted_cell = format_cell_value(
                    cell, row_index, total_rows, i, total_columns
                )

            # Apply styling and alignment based on column and content
            if i == 6:
                cells.append(f"<td class='column-yellow'>{formatted_cell}</td>")
            elif i == 0 and "I. Lợi nhuận trước thuế" in row_name:
                # Left-align first column cells with blue background
                cells.append(
                    f"<td class='cell-blue' style='text-align: left;'>{formatted_cell}</td>"
                )
            elif (
                i != 6
                and i != len(row) - 1
                and "II. Số lượng nhân sự ( Sale Manager )" in row_name
            ):
                cells.append(f"<td class='cell-yellow'>{formatted_cell}</td>")
            elif i == 0 and "III. Chỉ số tài chính" in row_name:
                # Left-align first column cells with orange background
                cells.append(
                    f"<td class='cell-orange' style='text-align: left;'>{formatted_cell}</td>"
                )
            elif i == 0 and row_name in green_rows:
                # Left-align first column cells with green background
                cells.append(
                    f"<td class='cell-green' style='text-align: left;'>{formatted_cell}</td>"
                )
            elif is_green_row and i != 0 and i != 6 and i != len(row) - 1:
                cells.append(f"<td class='cell-gray'>{formatted_cell}</td>")
            elif (
                row_name in ["I. Lợi nhuận trước thuế", "III. Chỉ số tài chính"]
                and i != 0
                and i != 6
                and i != len(row) - 1
            ):
                cells.append(f"<td class='cell-orange'>{formatted_cell}</td>")
            elif i == len(row) - 1:
                cells.append(f"<td class='cell-gray'>{formatted_cell}</td>")
            elif i == 0:
                # Left-align all first column cells with light gray background
                cells.append(
                    f"<td class='cell-light-gray' style='text-align: left;'>{formatted_cell}</td>"
                )
            else:
                cells.append(f"<td class='cell-light-yellow'>{formatted_cell}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    # Construct final HTML table
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
    df = get_ranking_data(period)
    if df.empty:
        return (
            f"<p>Không có dữ liệu cho kỳ báo cáo {period}. Vui lòng kiểm tra lại.</p>"
        )

    total_cols = len(df.columns)
    superheader_cells = []

    # === MODIFICATION START: Change first column header ===
    for i in range(total_cols):
        # Only build for the first 6 columns in this loop
        if i >= 6:
            continue

        column_name = df.columns[i]

        # Check if it's the first column
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
        elif column_name.lower() == "Xếp Hạng Cuối":
            superheader_cells.append(
                f"<th rowspan='2' class='rank-final-header'>{column_name}</th>"
            )
        else:
            superheader_cells.append(f"<th rowspan='2'>{column_name}</th>")

    # Superheader
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

        elif column_name.lower() in ["rank_ptkd", "rank_fin"]:
            subheader_cells.append(f"<th class='rank-green'>{column_name}</th>")
        elif "điểm quy mô" in column_name.lower() or "điểm fin" in column_name.lower():
            subheader_cells.append(f"<th class='diem-header'>{column_name}</th>")
        elif "tổng điểm" in column_name.lower():
            subheader_cells.append(f"<th class='tong-diem-header'>{column_name}</th>")
        elif column_name.lower() == "Xếp Hạng Cuối":
            subheader_cells.append(f"<th class='rank-final-header'>{column_name}</th>")
        else:
            subheader_cells.append(f"<th>{column_name}</th>")

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
        elif column_name.lower() in ["rank_ptkd", "rank_fin"]:
            subheader_cells.append(f"<th class='rank-green'>{column_name}</th>")
        elif "điểm quy mô" in column_name.lower() or "điểm fin" in column_name.lower():
            subheader_cells.append(f"<th class='diem-header'>{column_name}</th>")
        elif "tổng điểm" in column_name.lower():
            subheader_cells.append(f"<th class='tong-diem-header'>{column_name}</th>")
        elif column_name.lower() == "Xếp Hạng Cuối":
            subheader_cells.append(f"<th class='rank-final-header'>{column_name}</th>")
        else:
            subheader_cells.append(f"<th>{column_name}</th>")
    header_row = f"<tr>{''.join(subheader_cells)}</tr>"

    body_rows = []
    for _, row in df.iterrows():
        cells = []
        for i, cell in enumerate(row):
            column_name = df.columns[i]

            # === MODIFICATION START: Format first column's data ===
            if i == 0:
                # Format YYYYMM to MM/YYYY
                formatted_value = f"{str(cell)[4:]}/{str(cell)[:4]}"

                # extract month and year parts
                month_part = str(cell)[4:]
                year_part = "2025"
                formatted_value = f"{month_part}/{year_part}"
                cells.append(f"<td>{formatted_value}</td>")

            # === MODIFICATION END ===
            elif (
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
            # round to 8 decimal places
            elif column_name.lower() in [
                "approval_rate_avg",
                "npl_truoc_wo_luy_ke",
                "cir",
                "margin",
                "hs_von",
            ]:
                if pd.notnull(cell) and isinstance(cell, (int, float)):
                    formatted_value = f"{cell:,.8f}"
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
    report_month_display = request.args.get("month", "202505")
    report_month_data = report_month_display.replace("2025", "2023")
    summary_table_html = create_summary_html(report_month_data)
    months = [
        {"value": "202501", "text": "Tháng 1, 2025"},
        {"value": "202502", "text": "Tháng 2, 2025"},
        {"value": "202503", "text": "Tháng 3, 2025"},
        {"value": "202504", "text": "Tháng 4, 2025"},
        {"value": "202505", "text": "Tháng 5, 2025"},
    ]
    return render_template(
        "summary_report.html",
        summary_table_html=summary_table_html,
        months=months,
        selected_month=report_month_display,
    )


@app.route("/ranking_report")
def ranking_report():
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
    data = request.get_json()
    report_period = data.get("report_period")
    recipient_email = data.get("email")
    try:
        from run_all_reports import run_reports

        try:
            report_period_data = str(report_period).replace("2025", "2023")
            run_reports(report_period_data, recipient_email)
            report_generated = True
        except Exception as e:
            report_generated = False
            print(f"Error generating reports: {str(e)}")
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
