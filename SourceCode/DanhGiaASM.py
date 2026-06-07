# DanhGiaASM.py
# Data module for the "Đánh giá ASM" dashboard.
# Reuses the ASM ranking report function (fn_get_asm_ranking_report) and shapes
# the per-ASM rankings into a Chart.js-ready payload for the frontend.

import pandas as pd

from BaocaoXepHangASM_formatted import get_ranking_data

# How many months (current + prior) to show in the "Xếp hạng điểm tổng" table.
RANK_TABLE_MONTHS = 3

# The five ranking indicators shown in the "Top 10 - các chỉ số" table.
# (df column, display header)
TOP10_INDICATORS = [
    ("rank_final", "XH Tổng điểm"),
    ("rank_ltn_avg", "XH Dư nợ cho vay"),
    ("rank_psdn_avg", "XH Phát sinh dư nợ"),
    ("rank_approval_rate_avg", "XH Tỉ lệ hồ sơ KH được duyệt"),
    ("rank_npl_truoc_wo_luy_ke", "XH Tỷ lệ nợ xấu trước Write Off"),
]


def _empty_dashboard():
    """Return a safe, empty payload so the template never breaks."""
    return {
        "rank_table": {"months": [], "rows": []},
        "headcount": {"labels": [], "data": []},
        "top10": {"headers": [h for _, h in TOP10_INDICATORS], "rows": [], "total": []},
        "top10_regions": {"labels": [], "data": []},
        "insights": ["Không có dữ liệu cho kỳ báo cáo này."],
    }


def _prev_month_key(month_key):
    """202303 -> 202302, 202301 -> 202212."""
    yyyy, mm = month_key // 100, month_key % 100
    if mm == 1:
        return (yyyy - 1) * 100 + 12
    return yyyy * 100 + (mm - 1)


def _month_label(month_key, year_shift=0):
    """202303 -> 'Tháng 03/2025' (with year_shift = +2)."""
    yyyy = month_key // 100 + year_shift
    mm = month_key % 100
    return f"Tháng {mm:02d}/{yyyy}"


def _as_int(value):
    """Convert a ranking value to a plain int, or None if missing."""
    if value is None or pd.isna(value):
        return None
    return int(round(float(value)))


def _build_insights(top10_rows, region_counts):
    """Vietnamese commentary bullets matching the Power BI template style."""
    if not top10_rows:
        return ["Không có dữ liệu xếp hạng cho kỳ báo cáo này."]

    # Khu vực có nhiều ASM trong top 10 nhất.
    top_area = max(region_counts.items(), key=lambda kv: kv[1]) if region_counts else None

    bullets = [
        "Trong top 10 ASM theo xếp hạng tổng, tuy tỉ lệ trung bình lượng KH được "
        "duyệt không cao nhưng <strong>tỉ lệ phát sinh dư nợ luôn top đầu</strong>"
    ]
    if top_area:
        bullets.append(
            f"<strong>{top_area[0]}</strong> có số lượng ASM thuộc top 10 nhiều nhất "
            f"(<strong>{top_area[1]} ASM</strong>) — cho thấy mạng lưới khu vực này "
            "đang hoạt động hiệu quả"
        )
    bullets.append(
        "Các khu vực có lượng ASM không nhiều nhưng tỉ lệ ASM thuộc top 10 đều đặn "
        "<strong>quản lý tốt mạng lưới của họ</strong> — xem xét mở rộng đơn vị chi "
        "nhánh, mô hình kinh doanh khu vực này"
    )
    return bullets


def get_asm_evaluation_data(report_period, display_period=None):
    """
    Fetch and shape all data needed by the ASM evaluation dashboard.

    Args:
        report_period (str|int): selected reporting period in "YYYYMM" format.
        display_period (str|int): period as shown in the UI (for year-shift labels).

    Returns:
        dict: payload (rank_table / headcount / top10 / top10_regions / insights).
    """
    selected_key = int(report_period)

    # Label year-shift: DB 2023 -> UI 2025 (chỉ đổi label, không đổi dữ liệu).
    try:
        year_shift = (int(display_period) // 100) - (selected_key // 100) if display_period else 0
    except (TypeError, ValueError):
        year_shift = 0

    # --- Load the selected month + up to (RANK_TABLE_MONTHS - 1) prior months ---
    month_keys = [selected_key]
    for _ in range(RANK_TABLE_MONTHS - 1):
        month_keys.append(_prev_month_key(month_keys[-1]))
    month_keys = sorted(set(month_keys))  # ascending for the table columns

    frames = {}
    for mk in month_keys:
        df = get_ranking_data(mk)
        if df is not None and not df.empty:
            frames[mk] = df

    selected_df = frames.get(selected_key)
    if selected_df is None or selected_df.empty:
        return _empty_dashboard()

    # --- Rank table: rank_final per email across the loaded months ---
    available_months = [mk for mk in month_keys if mk in frames]
    rank_by_email = {}  # email -> {month_key: rank_final}
    area_by_email = {}
    for mk in available_months:
        for _, row in frames[mk].iterrows():
            email = row["email"]
            rank_by_email.setdefault(email, {})[mk] = _as_int(row["rank_final"])
            area_by_email[email] = row["area_name"]

    rank_rows = []
    for email, ranks in rank_by_email.items():
        rank_rows.append(
            {
                "email": email,
                "ranks": [ranks.get(mk) for mk in available_months],
            }
        )
    # Sort by the selected month's rank (ascending; missing rank sinks to bottom).
    sel_index = available_months.index(selected_key)
    rank_rows.sort(
        key=lambda r: (r["ranks"][sel_index] is None, r["ranks"][sel_index] or 0)
    )

    rank_table = {
        "months": [_month_label(mk, year_shift) for mk in available_months],
        "rows": rank_rows,
    }

    # --- Headcount donut: number of ASMs per area (selected month) ---
    hc = (
        selected_df.groupby("area_name")["email"]
        .nunique()
        .sort_values(ascending=False)
    )
    headcount = {"labels": list(hc.index), "data": [int(v) for v in hc.values]}

    # --- Top 10 by rank_final, with the 5 ranking indicators + totals ---
    top10_df = selected_df.sort_values("rank_final").head(10)
    top10_rows = []
    totals = [0] * len(TOP10_INDICATORS)
    region_counts = {}
    for _, row in top10_df.iterrows():
        values = []
        for i, (col, _) in enumerate(TOP10_INDICATORS):
            v = _as_int(row.get(col))
            values.append(v)
            if v is not None:
                totals[i] += v
        top10_rows.append({"email": row["email"], "values": values})
        area = row["area_name"]
        region_counts[area] = region_counts.get(area, 0) + 1

    top10 = {
        "headers": [h for _, h in TOP10_INDICATORS],
        "rows": top10_rows,
        "total": totals,
    }

    # --- Top-10 region distribution (bar, as % of the 10 ASMs) ---
    n_top = len(top10_rows) or 1
    sorted_regions = sorted(region_counts.items(), key=lambda kv: kv[1], reverse=True)
    top10_regions = {
        "labels": [a for a, _ in sorted_regions],
        "data": [round(c / n_top * 100, 1) for _, c in sorted_regions],
    }

    return {
        "rank_table": rank_table,
        "headcount": headcount,
        "top10": top10,
        "top10_regions": top10_regions,
        "insights": _build_insights(top10_rows, region_counts),
    }
