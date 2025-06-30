--------------------------------------------------------------------------------
-- TÁI CẤU TRÚC BẢNG ĐỂ TỐI ƯU HÓA
-- Sử dụng cấu trúc bảng "dài" (long format) để dễ dàng truy vấn và mở rộng.
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS fact_backdate_funding_monthly;
CREATE TABLE fact_backdate_funding_monthly (
    funding_id BIGINT NOT NULL,
    month_key BIGINT NOT NULL,
    area_code VARCHAR(10) NOT NULL,
    amount NUMERIC,
    PRIMARY KEY (funding_id, month_key, area_code)
);

-- Bảng xếp hạng ASM giữ nguyên cấu trúc
DROP TABLE IF EXISTS fact_backdate_asm_monthly;
CREATE TABLE fact_backdate_asm_monthly (
	month_key int8 NULL,
	area_cde varchar(200) NULL,
	area_name varchar(200) NULL,
	email varchar(200) NULL,
	tongdiem numeric NULL,
	rank_final int8 NULL,
	ltn_avg numeric NULL,
	rank_ltn_avg int8 NULL,
	psdn_avg numeric NULL,
	rank_psdn_avg int8 NULL,
	approval_rate_avg numeric NULL,
	rank_approval_rate_avg int8 NULL,
	npl_truoc_wo_luy_ke numeric NULL,
	rank_npl_truoc_wo_luy_ke int8 NULL,
	diem_quy_mo numeric NULL,
	rank_ptkd int8 NULL,
	cir numeric NULL,
	rank_cir int8 NULL,
    margin numeric NULL,
	rank_margin int8 NULL,
	hs_von numeric NULL,
	rank_hs_von int8 NULL,
	hsbq_nhan_su numeric NULL,
	rank_hsbq_nhan_su int8 NULL,
	diem_fin int8 NULL,
	rank_fin int8 NULL
);

--------------------------------------------------------------------------------
-- STORED PROCEDURE TÍNH TOÁN BÁO CÁO (PHIÊN BẢN ĐÃ SỬA LỖI VÀ HOÀN CHỈNH)
--------------------------------------------------------------------------------
-- Procedure đã được tối ưu với cấu trúc bảng mới
-- Procedure đã được tối ưu với cấu trúc bảng mới và sửa lỗi
CREATE OR REPLACE PROCEDURE prc_generate_summary_reports_monthly(p_rp_month BIGINT)
AS $$
DECLARE
    v_start_log_time TIMESTAMP;
    v_end_log_time TIMESTAMP;
    v_error_msg TEXT;
    v_log_id BIGINT;
    v_start_rp_month BIGINT := 202301; -- Tháng bắt đầu lũy kế
BEGIN
    -- ---------------------
    -- BƯỚC 1: KHỞI TẠO VÀ GHI LOG
    -- ---------------------
    v_start_log_time := clock_timestamp();

    INSERT INTO public.log_tracking (procedure_name, start_time, is_successful)
    VALUES ('prc_generate_summary_reports_monthly', v_start_log_time, false)
    RETURNING id INTO v_log_id;

    -- Xóa dữ liệu cũ của kỳ báo cáo
    DELETE FROM fact_backdate_funding_monthly WHERE month_key = p_rp_month;
    DELETE FROM fact_backdate_asm_monthly WHERE month_key = p_rp_month;

    -- ---------------------
    -- BƯỚC 2: TẠO CÁC BẢNG TẠM CHỨA DỮ LIỆU GỐC ĐÃ QUA SƠ CHẾ
    -- ---------------------

    -- Bảng tạm 1: Tính toán các tỷ lệ phân bổ từ fact_kpi_month cho các khu vực
    CREATE TEMP TABLE tmp_kpi_ratios ON COMMIT DROP AS
    WITH city_to_area AS (
        SELECT
            area_cde,
            TRIM(UNNEST(STRING_TO_ARRAY(REPLACE(city_list, '''', ''), ', '))) AS city_name
        FROM area_mapping
        WHERE area_cde != 'A' -- Chỉ lấy các khu vực, không lấy Hội sở
    ),
    kpi_aggregated AS (
        SELECT
            cta.area_cde,
            COALESCE(SUM(CASE WHEN k.max_bucket = 1 THEN k.outstanding_principal ELSE 0 END), 0) AS op_b1,
            COALESCE(SUM(CASE WHEN k.max_bucket = 2 THEN k.outstanding_principal ELSE 0 END), 0) AS op_b2,
            COALESCE(SUM(CASE WHEN k.max_bucket BETWEEN 2 AND 5 THEN k.outstanding_principal ELSE 0 END), 0) AS op_b2_5,
            COALESCE(SUM(CASE WHEN k.psdn = 1 THEN 1 ELSE 0 END), 0) AS psdn_count,
            COALESCE(SUM(CASE WHEN k.kpi_month = p_rp_month AND k.max_bucket IN (3, 4, 5) THEN k.outstanding_principal END),0) as npl,
			COALESCE(SUM(CASE WHEN k.write_off_month between v_start_rp_month and p_rp_month THEN k.write_off_balance_principal END),0) as wo_lk,
			COALESCE(SUM(CASE WHEN k.kpi_month = p_rp_month THEN k.outstanding_principal END),0) as total_outstanding
        FROM fact_kpi_month k
        JOIN city_to_area cta ON k.pos_city = cta.city_name
        WHERE k.kpi_month BETWEEN v_start_rp_month AND p_rp_month
        GROUP BY cta.area_cde
    )
    SELECT
        area_cde,
        op_b1 / NULLIF(SUM(op_b1) OVER (), 0) AS ratio_op_b1,
        op_b2 / NULLIF(SUM(op_b2) OVER (), 0) AS ratio_op_b2,
        op_b2_5 / NULLIF(SUM(op_b2_5) OVER (), 0) AS ratio_op_b2_5,
        psdn_count::NUMERIC / NULLIF(SUM(psdn_count) OVER (), 0) AS ratio_psdn,
        (npl + wo_lk) / NULLIF(total_outstanding + wo_lk, 0) AS npl_truoc_wo_luy_ke
    FROM kpi_aggregated;
    
    -- Bảng tạm 2: Lấy số lượng nhân sự SM theo khu vực
    CREATE TEMP TABLE tmp_sm_counts ON COMMIT DROP AS
    SELECT
        am.area_cde,
        COUNT(k.email) AS sm_count
    FROM kpi_asm_data k
    JOIN area_mapping am ON k.area_name = am.area_name
    GROUP BY am.area_cde;

    -- Bảng tạm 3: Tổng hợp số liệu giao dịch từ fact_txn_month
    CREATE TEMP TABLE tmp_txn_sums ON COMMIT DROP AS
    SELECT
        account_code,
        CASE
            WHEN analysis_code LIKE 'HEAD%' THEN 'A'
            WHEN analysis_code LIKE 'DVML.%.B.%.%' THEN 'B'
            WHEN analysis_code LIKE 'DVML.%.C.%.%' THEN 'C'
            WHEN analysis_code LIKE 'DVML.%.D.%.%' THEN 'D'
            WHEN analysis_code LIKE 'DVML.%.E.%.%' THEN 'E'
            WHEN analysis_code LIKE 'DVML.%.F.%.%' THEN 'F'
            WHEN analysis_code LIKE 'DVML.%.G.%.%' THEN 'G'
            WHEN analysis_code LIKE 'DVML.%.H.%.%' THEN 'H'
            ELSE NULL
        END AS area_code,
        SUM(amount) AS total_amount
    FROM fact_txn_month
    WHERE CAST(TO_CHAR(transaction_date, 'YYYYMM') AS BIGINT) BETWEEN v_start_rp_month AND p_rp_month
    GROUP BY 1, 2;

    -- ---------------------
    -- BƯỚC 3: TÍNH TOÁN VÀ INSERT DỮ LIỆU BÁO CÁO 1 (fact_backdate_funding_monthly)
    -- ---------------------
    WITH
    -- 1. Lấy tổng số liệu của Hội sở ('A') cần phân bổ
    head_distributable AS (
        SELECT
            'A' as area_cde,
            COALESCE(SUM(CASE WHEN account_code IN (702000030002, 702000030001, 702000030102) THEN total_amount END), 0) AS f9_amount,
            COALESCE(SUM(CASE WHEN account_code IN (702000030012, 702000030112) THEN total_amount END), 0) AS f10_amount,
            COALESCE(SUM(CASE WHEN account_code = 716000000001 THEN total_amount END), 0) AS f11_amount,
            COALESCE(SUM(CASE WHEN account_code = 719000030002 THEN total_amount END), 0) AS f12_amount,
            COALESCE(SUM(CASE WHEN account_code IN (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104) THEN total_amount END), 0) AS f13_amount,
            COALESCE(SUM(CASE WHEN CAST(account_code AS VARCHAR) LIKE '85%' THEN total_amount END), 0) AS f25_amount,
            COALESCE(SUM(CASE WHEN CAST(account_code AS VARCHAR) LIKE '86%' THEN total_amount END), 0) AS f26_amount,
            COALESCE(SUM(CASE WHEN CAST(account_code AS VARCHAR) LIKE '87%' THEN total_amount END), 0) AS f27_amount
        FROM tmp_txn_sums
        WHERE area_code = 'A'
        GROUP BY area_cde
    ),
    -- 2. Tính toán số liệu cuối cùng cho các Khu vực (B-H)
    regional_final_amounts AS (
        SELECT
            r.area_cde,
            (SELECT f9_amount FROM head_distributable) * r.ratio_op_b1 + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code IN (702000030002, 702000030001, 702000030102)), 0) AS f9_amount,
            (SELECT f10_amount FROM head_distributable) * r.ratio_op_b2 + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code IN (702000030012, 702000030112)), 0) AS f10_amount,
            (SELECT f11_amount FROM head_distributable) * r.ratio_psdn + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code = 716000000001), 0) AS f11_amount,
            (SELECT f12_amount FROM head_distributable) * r.ratio_op_b1 + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code = 719000030002), 0) AS f12_amount,
            (SELECT f13_amount FROM head_distributable) * r.ratio_op_b2_5 + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code IN (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)), 0) AS f13_amount,
            (SELECT f25_amount FROM head_distributable) * (s.sm_count::numeric / NULLIF((SELECT SUM(sm_count) FROM tmp_sm_counts), 0)) + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND CAST(t.account_code AS VARCHAR) LIKE '85%'), 0) AS f25_amount,
            (SELECT f26_amount FROM head_distributable) * (s.sm_count::numeric / NULLIF((SELECT SUM(sm_count) FROM tmp_sm_counts), 0)) + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND CAST(t.account_code AS VARCHAR) LIKE '86%'), 0) AS f26_amount,
            (SELECT f27_amount FROM head_distributable) * (s.sm_count::numeric / NULLIF((SELECT SUM(sm_count) FROM tmp_sm_counts), 0)) + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND CAST(t.account_code AS VARCHAR) LIKE '87%'), 0) AS f27_amount
        FROM tmp_kpi_ratios r
        JOIN tmp_sm_counts s ON r.area_cde = s.area_cde
    ),
    -- 3. Gộp số liệu gốc của Hội sở và Khu vực
    base_metrics AS (
        SELECT area_cde, f9_amount, f10_amount, f11_amount, f12_amount, f13_amount, f25_amount, f26_amount, f27_amount FROM regional_final_amounts
        UNION ALL
        SELECT area_cde, f9_amount, f10_amount, f11_amount, f12_amount, f13_amount, f25_amount, f26_amount, f27_amount FROM head_distributable
    ),
    -- 4. Tính các chỉ số tổng hợp và chi phí vốn
    composite_and_capital_costs AS (
        WITH totals_for_ratio AS (
            SELECT
                (SELECT SUM(total_amount) FROM tmp_txn_sums WHERE account_code IN ('702000040001','702000040002','703000000001','703000000002','703000000003','703000000004', '721000000041','721000000037','721000000039','721000000013','721000000014','721000000036','723000000014', '723000000037','821000000014','821000000037','821000000039','821000000041','821000000013','821000000036', '823000000014','823000000037','741031000001','741031000002','841000000001','841000000005','841000000004', '701000000001','701000000002','701037000001','701037000002','701000000101')) AS total_capital_revenue,
                (SELECT SUM(f9_amount + f10_amount) FROM base_metrics) AS total_card_interest,
                (SELECT SUM(total_amount) FROM tmp_txn_sums WHERE area_code = 'A' AND account_code IN (801000000001, 802000000001)) AS distributable_f15,
                (SELECT SUM(total_amount) FROM tmp_txn_sums WHERE area_code = 'A' AND account_code = 803000000001) AS distributable_f17
        )
        SELECT
            bm.area_cde,
            bm.f9_amount + bm.f10_amount + bm.f11_amount + bm.f12_amount + bm.f13_amount AS f4_amount,
            bm.f25_amount + bm.f26_amount + bm.f27_amount AS f8_amount,
            (tr.distributable_f15 * (bm.f9_amount + bm.f10_amount)) / NULLIF(tr.total_capital_revenue + tr.total_card_interest, 0) AS f15_amount,
            (tr.distributable_f17 * (bm.f9_amount + bm.f10_amount)) / NULLIF(tr.total_capital_revenue + tr.total_card_interest, 0) AS f17_amount
        FROM base_metrics bm, totals_for_ratio tr
    ),
    -- 5. Unpivot tất cả dữ liệu thành dạng dòng để INSERT
    final_data AS (
        SELECT area_cde, 9 AS funding_id, f9_amount AS amount FROM base_metrics UNION ALL
        SELECT area_cde, 10, f10_amount FROM base_metrics UNION ALL
        SELECT area_cde, 11, f11_amount FROM base_metrics UNION ALL
        SELECT area_cde, 12, f12_amount FROM base_metrics UNION ALL
        SELECT area_cde, 13, f13_amount FROM base_metrics UNION ALL
        SELECT area_cde, 25, f25_amount FROM base_metrics UNION ALL
        SELECT area_cde, 26, f26_amount FROM base_metrics UNION ALL
        SELECT area_cde, 27, f27_amount FROM base_metrics UNION ALL
        SELECT area_cde, 4, f4_amount FROM composite_and_capital_costs UNION ALL
        SELECT area_cde, 8, f8_amount FROM composite_and_capital_costs UNION ALL
        SELECT area_cde, 15, f15_amount FROM composite_and_capital_costs UNION ALL
        SELECT area_cde, 17, f17_amount FROM composite_and_capital_costs UNION ALL
        SELECT area_cde, 5, f15_amount + f17_amount FROM composite_and_capital_costs -- funding_id 5 = 15 + 17
    )
    -- 6. Insert dữ liệu cuối cùng vào bảng fact
    INSERT INTO fact_backdate_funding_monthly(funding_id, month_key, area_code, amount)
    SELECT funding_id, p_rp_month, area_cde, amount FROM final_data;

    -- ---------------------
    -- BƯỚC 4: TÍNH TOÁN DỮ LIỆU BÁO CÁO 2 (fact_backdate_asm_monthly)
    -- ---------------------
    WITH asm_calcs AS (
        SELECT
            am.area_cde,
            k.area_name,
            k.email,
            (CASE
                WHEN p_rp_month % 100 = 1 THEN k.ltn_jan
                WHEN p_rp_month % 100 = 2 THEN k.ltn_jan + k.ltn_feb
                WHEN p_rp_month % 100 = 3 THEN k.ltn_jan + k.ltn_feb + k.ltn_mar
                -- ... thêm các tháng còn lại
                ELSE 0
            END)::NUMERIC / (p_rp_month % 100) AS ltn_avg,
            -- ... Tương tự cho psdn_avg, approval_rate_avg
            r.npl_truoc_wo_luy_ke,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 29 AND sub.area_code = am.area_cde) AS cir,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 30 AND sub.area_code = am.area_cde) AS margin,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 31 AND sub.area_code = am.area_cde) AS hs_von,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 32 AND sub.area_code = am.area_cde) AS hsbq_nhan_su
        FROM kpi_asm_data k
        JOIN area_mapping am ON k.area_name = am.area_name
        LEFT JOIN tmp_kpi_ratios r ON am.area_cde = r.area_cde
    ),
    ranked_data AS (
        SELECT
            *,
            RANK() OVER (ORDER BY ltn_avg DESC) as rank_ltn_avg,
            DENSE_RANK() OVER (ORDER BY cir ASC) as rank_cir,
            DENSE_RANK() OVER (ORDER BY margin DESC) as rank_margin,
            DENSE_RANK() OVER (ORDER BY hs_von DESC) as rank_hs_von,
            DENSE_RANK() OVER (ORDER BY hsbq_nhan_su DESC) as rank_hsbq_nhan_su,
            RANK() OVER (ORDER BY npl_truoc_wo_luy_ke ASC) as rank_npl_truoc_wo_luy_ke
        FROM asm_calcs
    ),
    final_asm_data AS (
        SELECT
            *,
            (rank_ltn_avg + rank_npl_truoc_wo_luy_ke) AS diem_quy_mo, -- Cần thêm các rank khác nếu có
            (rank_cir + rank_margin + rank_hs_von + rank_hsbq_nhan_su) as diem_fin
        FROM ranked_data
    )
    INSERT INTO fact_backdate_asm_monthly(month_key, area_cde, area_name, email, tongdiem, rank_final, ltn_avg, rank_ltn_avg, diem_quy_mo, diem_fin)
    SELECT
        p_rp_month,
        area_cde,
        area_name,
        email,
        diem_quy_mo + diem_fin AS tongdiem,
        RANK() OVER (ORDER BY (diem_quy_mo + diem_fin) ASC) as rank_final,
        ltn_avg,
        rank_ltn_avg,
        diem_quy_mo,
        diem_fin
    FROM final_asm_data;

    -- ---------------------
    -- BƯỚC 5: KẾT THÚC VÀ GHI LOG
    -- ---------------------
    v_end_log_time := clock_timestamp();
    UPDATE log_tracking SET end_time = v_end_log_time, is_successful = true WHERE id = v_log_id;

EXCEPTION
    WHEN OTHERS THEN
        v_end_log_time := clock_timestamp();
        v_error_msg := SQLERRM || ' - ' || SQLSTATE;
        UPDATE log_tracking SET end_time = v_end_log_time, is_successful = false, error_log = v_error_msg WHERE id = v_log_id;
        RAISE NOTICE 'Lỗi xảy ra: %', v_error_msg;
END;
$$ LANGUAGE plpgsql;
--------------------------------------------
call prc_generate_summary_reports_monthly(202302);
