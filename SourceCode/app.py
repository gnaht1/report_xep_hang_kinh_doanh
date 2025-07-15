# In your app.py file
import math


import pandas as pd
import numpy as np  # Make sure to import numpy
import json
import plotly
import plotly.graph_objects as go
from flask import Flask, render_template, request, jsonify

# (Keep your other imports for get_summary_data, get_ranking_data, etc.)
from BaocaoTonghop_formatted import get_summary_data
from BaocaoXepHangASM_formatted import get_ranking_data
from run_all_reports import send_email
import config

app = Flask(__name__)


# def create_summary_table(period):
#     df = get_summary_data(period)
#     if df.empty:
#         return None

#     numeric_cols = df.columns[1:]
#     rows_to_transform = df.index[:-6].union(df.index[-1:])

#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors="coerce")
#         df.loc[rows_to_transform, col] = df.loc[rows_to_transform, col] / 1000000

#     for col in numeric_cols:

#         def format_value(val):
#             if pd.isna(val):
#                 return "-"
#             if not isinstance(val, (int, float)):
#                 return val
#             if math.isclose(val, 0):
#                 return "-"
#             return f"{val:,.2f}"

#         df[col] = df[col].apply(format_value)

#     # THÊM CỘT TRỐNG GIỮA CỘT 6 và 7
#     col_name = " "
#     insert_at = 6
#     df.insert(insert_at, col_name, "")

#     num_cols = len(df.columns)
#     num_rows = len(df)

#     # Fill màu: cột mới màu vàng, còn lại lavender
#     fill_color = []
#     for col_idx in range(num_cols):
#         if col_idx == 6:
#             fill_color.append(["#fff699"] * num_rows)  # vàng nhạt cho cột 7 mới
#         else:
#             fill_color.append(["lavender"] * num_rows)

#     fig = go.Figure(
#         data=[
#             go.Table(
#                 columnwidth=[
#                     100,
#                     40,
#                     40,
#                     40,
#                     40,
#                     40,
#                     30,
#                     40,
#                     40,
#                     40,
#                     40,
#                     40,
#                     40,
#                     40,
#                     40,
#                 ],
#                 header=dict(
#                     values=list(df.columns),
#                     fill_color="paleturquoise",
#                     align="center",
#                     font=dict(size=12, color="black"),
#                 ),
#                 cells=dict(
#                     values=[df[col] for col in df.columns],
#                     fill_color=fill_color,
#                     align="left",
#                     height=30,
#                 ),
#             )
#         ]
#     )
#     fig.update_layout(
#         autosize=False,
#         width=1550,
#         height=600,
#         margin=dict(l=10, r=10, t=0, b=10),
#     )

#     return fig


# app.py (new function)
def create_summary_html(period):
    df = get_summary_data(period)

    if df.empty:
        return "<p>No data available.</p>"

    # Thêm cột trống giữa cột 6 và 7
    df.insert(6, " ", "")

    # Đếm số cột để xác định colspan chính xác
    total_cols = len(df.columns)
    print(f"Tổng số cột: {total_cols}")
    print(f"Tên các cột: {list(df.columns)}")

    # Build superheader row:
    # Giả sử có 15 cột tổng cộng:
    # - Cột 1: tên hàng (rowspan=2)
    # - Cột 2-6: "Tổng cần phân bổ xuống cho ĐVML" (colspan=5)
    # - Cột 7: cột trống (rowspan=2)
    # - Cột 8-14: "KHU VỰC MẠNG LƯỚI" (colspan=7)
    # - Cột 15: cột cuối (rowspan=2)

    superheader = (
        "<tr>"
        f"<th rowspan='2'>{df.columns[0]}</th>"
        "<th colspan='5' class='header-blue'>Tổng cần phân bổ xuống cho ĐVML</th>"
        "<th rowspan='2' class='header-yellow'></th>"
        "<th colspan='7' class='header-blue' >KHU VỰC MẠNG LƯỚI</th>"
        f"<th rowspan='2'>{df.columns[-1]}</th>"
        "</tr>"
    )

    # Build the second header row - chỉ cần các cột từ 2-6 và 8-14
    # Bỏ qua cột 1 (index 0), cột 7 (index 6), và cột cuối (index -1)
    middle_columns = []
    for i, col in enumerate(df.columns):
        if (
            i == 0 or i == 6 or i == len(df.columns) - 1
        ):  # Bỏ qua cột đầu, cột trống, và cột cuối
            continue
        middle_columns.append(col)

    subheaders = "".join(f"<th>{col}</th>" for col in middle_columns)
    header_row = f"<tr>{subheaders}</tr>"

    # Green cells
    green_rows = [
        "Thu nhập từ hoạt động thẻ",
        "Chi phí thuần KDV",
        "Chi phí thuần hoạt động khác",
        "Tổng thu nhập hoạt động",
        "Tổng chi phí hoạt động",
        "Chi phí dự phòng",
    ]
    # Build table body
    body_rows = []
    for _, row in df.iterrows():
        cells = []
        row_name = str(row.iloc[0]).strip()  # Lấy tên hàng (cột đầu tiên)
        is_green_row = row_name in green_rows

        for i, cell in enumerate(row):
            if i == 6:  # Cột trống thứ 7 (index 6) - màu vàng
                cells.append(f"<td class='column-yellow'>{cell}</td>")
            elif (
                i == 0 and "Lợi nhuận trước thuế" in row_name
            ):  # Ô đầu tiên của hàng "1. Lợi nhuận trước thuế"
                cells.append(f"<td class='cell-blue'>{cell}</td>")
            elif i == 0 and "Số lượng nhân sự ( Sale Manager )" in row_name:
                cells.append(f"<td class='cell-yellow'>{cell}</td>")
            elif i == 0 and "Chỉ số tài chính" in row_name:
                cells.append(f"<td class='cell-orange'>{cell}</td>")
            elif i == 0 and row_name in green_rows:
                cells.append(f"<td class='cell-green'>{cell}</td>")
            elif is_green_row and i != 0 and i != 6 and i != len(row) - 1:
                # Cột từ 2-6 (index 1-5) và 8-14 (index 7-13) của green_rows - màu xám
                # Bỏ qua cột đầu (i==0), cột trống (i==6), và cột cuối (i==len(row)-1)
                cells.append(f"<td class='cell-gray'>{cell}</td>")
            else:
                cells.append(f"<td>{cell}</td>")

        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    html = (
        "<table class='summary-table'>"
        "<thead>"
        f"{superheader}"
        f"{header_row}"
        "</thead>"
        "<tbody>" + "".join(body_rows) + "</tbody>"
        "</table>"
    )
    return html


# --- HÀM create_ranking_table VÀ CÁC ROUTE CÒN LẠI GIỮ NGUYÊN ---


def create_ranking_table(period):
    # This function does not need changes for this request.
    df = get_ranking_data(period)
    if df.empty:
        return None
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df.columns), fill_color="lightgreen", align="left"
                ),
                cells=dict(
                    values=[df[col] for col in df.columns],
                    fill_color="white",
                    align="left",
                ),
            )
        ]
    )
    return fig


@app.route("/")
def index():
    report_month = request.args.get("month", "202305")
    summary_table_html = create_summary_html(report_month)

    ranking_fig = create_ranking_table(report_month)
    summary_graph_json = (
        json.dumps(summary_table_html, cls=plotly.utils.PlotlyJSONEncoder)
        if summary_table_html
        else "null"
    )
    ranking_graph_json = (
        json.dumps(ranking_fig, cls=plotly.utils.PlotlyJSONEncoder)
        if ranking_fig
        else "null"
    )
    months = [
        {"value": "202301", "text": "Tháng 1, 2023"},
        {"value": "202302", "text": "Tháng 2, 2023"},
        {"value": "202303", "text": "Tháng 3, 2023"},
        {"value": "202304", "text": "Tháng 4, 2023"},
        {"value": "202305", "text": "Tháng 5, 2023"},
    ]
    return render_template(
        "index.html",
        summary_table_html=summary_table_html,
        ranking_graph_json=ranking_graph_json,
        months=months,
        selected_month=report_month,
    )


@app.route("/send-approval-email", methods=["POST"])
def send_approval_email():
    data = request.get_json()
    report_period = data.get("report_period")
    recipient_email = getattr(config, "MANAGER_EMAIL", "manager@example.com")
    subject = f"✅ Phê duyệt Báo cáo tháng {report_period}"
    body = f"""
    <html><body>
        <p>Xin chào cấp trên,</p>
        <p>Báo cáo cho kỳ <strong>{report_period}</strong> đã được xem xét và phê duyệt.</p>
        <p>Bạn có thể xem trực tiếp báo cáo tại đây:</p>
        <p><a href="{request.host_url}?month={report_period}">Xem Báo cáo Tương tác</a></p>
        <p>Trân trọng.</p>
    </body></html>
    """
    try:
        send_email(subject, body, recipient_email)
        return jsonify(
            {"status": "success", "message": f"Email đã được gửi tới {recipient_email}"}
        )
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


if __name__ == "__main__":
    app.run(debug=True)
