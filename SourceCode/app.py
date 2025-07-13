import json
import plotly
import plotly.graph_objects as go
from flask import Flask, render_template, request, jsonify

# Import các hàm lấy dữ liệu đã tạo ở bước 1
from BaocaoTonghop_formatted import get_summary_data
from BaocaoXepHangASM_formatted import get_ranking_data

# Import hàm gửi email từ file gốc
from run_all_reports import send_email
import config  # Import config để lấy thông tin email

app = Flask(__name__)


# --- Hàm tạo biểu đồ Plotly ---
def create_summary_table(period):
    df = get_summary_data(period)
    if df.empty:
        return None
    # TẠO BẢNG PLOTLY: Giao diện sẽ được tùy chỉnh ở đây
    # Đây là ví dụ cơ bản, bạn cần tùy chỉnh màu sắc, font chữ để giống Excel
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df.columns), fill_color="paleturquoise", align="left"
                ),
                cells=dict(
                    values=[df[col] for col in df.columns],
                    fill_color="lavender",
                    align="left",
                ),
            )
        ]
    )
    return fig


def create_ranking_table(period):
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


# --- Routes (Đường dẫn của web) ---
@app.route("/")
def index():
    # Lấy tháng từ request, mặc định là tháng 5/2023
    report_month = request.args.get("month", "202305")

    # Tạo biểu đồ
    summary_fig = create_summary_table(report_month)
    ranking_fig = create_ranking_table(report_month)

    # Chuyển biểu đồ thành JSON để hiển thị trên web
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

    # Danh sách các tháng để chọn
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
    # Thêm email của cấp trên vào file config.py
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
