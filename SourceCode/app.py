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


def create_summary_table(period):
    df = get_summary_data(period)
    if df.empty:
        return None

    numeric_cols = df.columns[1:]
    rows_to_transform = df.index[:-6].union(df.index[-1:])

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[rows_to_transform, col] = df.loc[rows_to_transform, col] / 1000000

    for col in numeric_cols:

        def format_value(val):
            if pd.isna(val):
                return "-"
            if not isinstance(val, (int, float)):
                return val
            if math.isclose(val, 0):
                return "-"
            return f"{val:,.2f}"

        df[col] = df[col].apply(format_value)

    # --- Chỉ tạo fill_color mặc định cho tất cả các dòng ---
    num_cols = len(df.columns)
    fill_color = [["lavender"] * len(df) for _ in range(num_cols)]

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df.columns),
                    fill_color="paleturquoise",
                    align="center",
                    font=dict(size=12, color="black"),
                ),
                cells=dict(
                    values=[df[col] for col in df.columns],
                    fill_color=fill_color,
                    align="left",
                    height=30,
                ),
            )
        ]
    )
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    return fig


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
    summary_fig = create_summary_table(report_month)
    ranking_fig = create_ranking_table(report_month)
    summary_graph_json = (
        json.dumps(summary_fig, cls=plotly.utils.PlotlyJSONEncoder)
        if summary_fig
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
        summary_graph_json=summary_graph_json,
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
