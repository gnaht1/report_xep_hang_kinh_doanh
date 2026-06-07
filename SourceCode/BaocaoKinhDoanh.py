# BaocaoKinhDoanh.py
# Data module for the "Báo cáo Kinh doanh" dashboard.
# Fetches KPI + trend data from PostgreSQL and shapes it for Chart.js on the frontend.

import psycopg2
import pandas as pd

# Same connection params used by the other report modules.
DB_PARAMS = {
    "host": "localhost",
    "port": "5432",
    "dbname": "final_project",
    "user": "postgres",
    "password": "1234",
}

# Order of the 7 network areas (B-H) shown in the comparison charts.
REGION_ORDER = ["B", "C", "D", "E", "F", "G", "H"]


def _empty_dashboard():
    """Return a safe, empty payload so the template never breaks."""
    return {
        "kpi": {
            "doanh_thu": 0.0,
            "chi_phi": 0.0,
            "chi_phi_dp": 0.0,
            "loi_nhuan": 0.0,
            "nhan_su": 0,
        },
        "regions": {
            "labels": [],
            "loi_nhuan": [],
            "margin": [],
            "cir": [],
            "doanh_thu": [],
            "chi_phi": [],
            "chi_phi_dp": [],
            "nhan_su": [],
        },
        "trend": {"labels": [], "series": []},
        "insights": ["Không có dữ liệu cho kỳ báo cáo này."],
    }


def _build_insights(kpi_df, region_rows):
    """Generate Vietnamese commentary bullets matching the Power BI template style."""
    if not region_rows:
        return ["Không có dữ liệu khu vực cho kỳ báo cáo này."]

    # Khu vực có lợi nhuận cao nhất (mặc định coi là "ngôi sao" của kỳ).
    best = max(region_rows, key=lambda r: r["loi_nhuan"])

    # Xếp hạng nhân sự (ASM) để lấy thứ hạng + % của khu vực best.
    total_hc = sum(r["nhan_su"] for r in region_rows) or 1
    sorted_by_hc = sorted(region_rows, key=lambda r: r["nhan_su"], reverse=True)
    hc_rank = next(
        (i + 1 for i, r in enumerate(sorted_by_hc) if r["area_code"] == best["area_code"]),
        len(region_rows),
    )
    hc_pct = best["nhan_su"] / total_hc * 100

    rank_word = {1: "nhất", 2: "nhì", 3: "ba", 4: "tư", 5: "năm", 6: "sáu", 7: "bảy"}.get(
        hc_rank, str(hc_rank)
    )

    return [
        "Sự phân bổ các nguồn thu/chi giữa các khu vực <strong>không đồng đều</strong>",
        (
            "Nhìn chung các khu vực trong khoảng thời gian này đều có "
            "<strong>mức độ tăng trưởng ổn định</strong>, đặc biệt là khu vực "
            f"<strong>{best['area_name']}</strong>"
        ),
        (
            f"<strong>{best['area_name']}</strong> là khu vực <strong>hoạt động hiệu quả</strong> "
            f"nhất quý này với lợi nhuận, tổng thu nhập cao nhất tuy số lượng nhân sự (ASM) "
            f"chỉ đứng thứ {rank_word} (chiếm ~{hc_pct:.0f}%)"
        ),
    ]


def get_business_dashboard_data(report_period, area_code="ALL"):
    """
    Fetch and shape all data needed by the business dashboard.

    Args:
        report_period (str|int): reporting period in "YYYYMM" format.
        area_code (str): "ALL" (consolidated, area 'A') or a single area code (B-H).

    Returns:
        dict: Chart.js-ready payload (kpi / regions / trend / insights).
    """
    try:
        connection = psycopg2.connect(**DB_PARAMS)
        kpi_df = pd.read_sql_query(
            f"SELECT * FROM fn_get_business_dashboard_kpi({report_period});", connection
        )
        trend_df = pd.read_sql_query(
            f"SELECT * FROM fn_get_business_dashboard_trend({report_period});",
            connection,
        )
        connection.close()
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu dashboard kinh doanh: {e}")
        return _empty_dashboard()

    if kpi_df.empty:
        return _empty_dashboard()

    kpi_by_area = {row["area_code"]: row for _, row in kpi_df.iterrows()}

    # --- KPI cards ---
    # "ALL" reads the consolidated 'A' row; nếu không có → cộng dồn B-H làm fallback.
    # Chi phí trong DB lưu số âm; hiển thị dưới dạng dương (abs) cho KPI card.
    if area_code == "ALL":
        kpi_row = kpi_by_area.get("A")
        if kpi_row is None:
            region_only = kpi_df[kpi_df["area_code"].isin(REGION_ORDER)]
            if not region_only.empty:
                summed = region_only.sum(numeric_only=True)
                kpi_row = {
                    "doanh_thu": summed.get("doanh_thu", 0),
                    "chi_phi": summed.get("chi_phi", 0),
                    "chi_phi_dp": summed.get("chi_phi_dp", 0),
                    "loi_nhuan": summed.get("loi_nhuan", 0),
                    "nhan_su": summed.get("nhan_su", 0),
                }
    else:
        kpi_row = kpi_by_area.get(area_code)

    if kpi_row is None:
        kpi = _empty_dashboard()["kpi"]
    else:
        kpi = {
            "doanh_thu": float(kpi_row["doanh_thu"]) / 1e9,
            "chi_phi": abs(float(kpi_row["chi_phi"])) / 1e9,
            "chi_phi_dp": abs(float(kpi_row["chi_phi_dp"])) / 1e9,
            "loi_nhuan": float(kpi_row["loi_nhuan"]) / 1e9,
            "nhan_su": int(round(float(kpi_row["nhan_su"]))),
        }

    # --- Region comparison series ---
    # Nếu user chọn 1 khu vực cụ thể → chỉ hiển thị khu vực đó trên các chart so sánh.
    # ALL → hiển thị toàn bộ B-H.
    region_codes = REGION_ORDER if area_code == "ALL" else [area_code]
    region_rows = []
    for code in region_codes:
        row = kpi_by_area.get(code)
        if row is None:
            continue
        region_rows.append(
            {
                "area_code": code,
                "area_name": row["area_name"],
                "loi_nhuan": float(row["loi_nhuan"]),
                "margin": float(row["margin"]),
                "cir": float(row["cir"]),
                "doanh_thu": float(row["doanh_thu"]),
                # Chi phí: DB lưu số âm → hiển thị dạng dương cho bar/stacked.
                "chi_phi": abs(float(row["chi_phi"])),
                "chi_phi_dp": abs(float(row["chi_phi_dp"])),
                "nhan_su": int(round(float(row["nhan_su"]))),
            }
        )

    regions = {
        "labels": [r["area_name"] for r in region_rows],
        "loi_nhuan": [r["loi_nhuan"] for r in region_rows],
        "margin": [r["margin"] for r in region_rows],
        "cir": [r["cir"] for r in region_rows],
        "doanh_thu": [r["doanh_thu"] for r in region_rows],
        "chi_phi": [r["chi_phi"] for r in region_rows],
        "chi_phi_dp": [r["chi_phi_dp"] for r in region_rows],
        "nhan_su": [r["nhan_su"] for r in region_rows],
    }

    # --- Trend (one line per area, x-axis = month_key) ---
    trend = {"labels": [], "series": []}
    if not trend_df.empty:
        months = sorted(trend_df["month_key"].unique())
        trend["labels"] = [str(int(m)) for m in months]
        trend_codes = REGION_ORDER if area_code == "ALL" else [area_code]
        for code in trend_codes:
            area_slice = trend_df[trend_df["area_code"] == code]
            if area_slice.empty:
                continue
            by_month = dict(
                zip(area_slice["month_key"], area_slice["tong_thu_nhap"])
            )
            name = area_slice.iloc[0]["area_name"]
            trend["series"].append(
                {
                    "name": name,
                    "data": [float(by_month.get(m, 0) or 0) for m in months],
                }
            )

    return {
        "kpi": kpi,
        "regions": regions,
        "trend": trend,
        "insights": _build_insights(kpi_df, region_rows),
    }
