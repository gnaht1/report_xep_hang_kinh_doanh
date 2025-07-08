-- Tổ chức mô hình dữ liệu dim , fact để lưu trữ một cách tối ưu ( tái sử dụng )
CREATE TABLE dim_funding_structure (
    funding_id SERIAL PRIMARY KEY,
    funding_code VARCHAR(255) NOT NULL,
    funding_name VARCHAR(255) NOT NULL,
    funding_parent_id INT,
    funding_level INT,
    sortorder int,
    rec_created_dt timestamp default now(),
    rec_updated_dt timestamp default now()
);

select * from dim_funding_structure
order by sortorder;

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

-- Tao bang area_mapping
CREATE TABLE IF NOT EXISTS area_mapping (
    area_cde VARCHAR(10) PRIMARY KEY,
    area_name VARCHAR(50) NOT NULL
);

INSERT INTO area_mapping (area_cde, area_name) VALUES
    ('A', 'Hội Sở'),
    ('B', 'Đông Bắc Bộ'),
    ('C', 'Tây Bắc Bộ'),
    ('D', 'Đồng Bằng Sông Hồng'),
    ('E', 'Bắc Trung Bộ'),
    ('F', 'Nam Trung Bộ'),
    ('G', 'Tây Nam Bộ'),
    ('H', 'Đông Nam Bộ');
   
ALTER TABLE public.area_mapping ADD city_list varchar NULL;

UPDATE area_mapping
SET city_list = CASE area_cde
	WHEN 'B' THEN '''Hà Giang'', ''Tuyên Quang'', ''Phú Thọ'', ''Thái Nguyên'', ''Bắc Kạn'', ''Cao Bằng'', ''Lạng Sơn'', ''Bắc Giang'', ''Quảng Ninh'''
	WHEN 'C' THEN '''Lào Cai'', ''Yên Bái'', ''Điện Biên'', ''Sơn La'', ''Hòa Bình'''
	WHEN 'D' THEN '''Hà Nội'', ''Hải Phòng'', ''Vĩnh Phúc'', ''Bắc Ninh'', ''Hưng Yên'', ''Hải Dương'', ''Thái Bình'', ''Nam Định'', ''Ninh Bình'', ''Hà Nam'''
	WHEN 'E' THEN '''Thanh Hoá'', ''Nghệ An'', ''Hà Tĩnh'', ''Quảng Bình'', ''Quảng Trị'', ''Huế'''
	WHEN 'F' THEN '''Đà Nẵng'', ''Quảng Nam'', ''Quảng Ngãi'', ''Bình Định'', ''Phú Yên'', ''Khánh Hoà'', ''Ninh Thuận'', ''Bình Thuận'', ''Kon Tum'', ''Gia Lai'', ''Đắk Lắk'', ''Đắk Nông'', ''Lâm Đồng'''
	WHEN 'G' THEN '''Cần Thơ'', ''Long An'', ''Đồng Tháp'', ''Tiền Giang'', ''An Giang'', ''Bến Tre'', ''Vĩnh Long'', ''Trà Vinh'', ''Hậu Giang'', ''Kiên Giang'', ''Sóc Trăng'', ''Bạc Liêu'', ''Cà Mau'''
	WHEN 'H' THEN '''Hồ Chí Minh'', ''Bà Rịa - Vũng Tàu'', ''Bình Dương'', ''Bình Phước'', ''Đồng Nai'', ''Tây Ninh'''
	ELSE ''
END;


------------- Bang log
create table log_tracking(
	id serial primary key 
	, procedure_name text not null 
	, start_time timestamp 
	, end_time timestamp 
	, is_successful bool
	, error_log text 
	, rec_created_dt timestamp default now()
);

-- index 
CREATE INDEX fact_txn_month_account_code_annalysis_code_trans_date_idx ON public.fact_txn_month (account_code,analysis_code,transaction_date);
CREATE INDEX fact_kpi_month_pos_city_monthkey_idx ON public.fact_kpi_month (pos_city,kpi_month);
CREATE INDEX fact_kpi_month_kpi_month_maxbucket_poscity_idx ON public.fact_kpi_month (kpi_month,pos_city,max_bucket);
CREATE INDEX fact_kpi_month_kpi_month_maxbucket_idx ON public.fact_kpi_month (kpi_month,max_bucket);
CREATE INDEX fact_kpi_month_pos_city_writeoff_month_idx ON public.fact_kpi_month (pos_city,write_off_month);
--------------------------------------------------------------------------------
-- STORED PROCEDURE TÍNH TOÁN BÁO CÁO 
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE prc_generate_summary_reports_monthly(p_rp_month BIGINT)
AS $$
DECLARE
    v_start_log_time TIMESTAMP;
    v_end_log_time TIMESTAMP;
    v_error_msg TEXT;
    v_log_id BIGINT;
    v_start_rp_month BIGINT := 202301; -- Tháng bắt đầu lũy kế
    v_ltn_column TEXT;
    v_psdn_column TEXT;
    v_approved_rate_column TEXT;
    v_month_num INT := p_rp_month % 100;
BEGIN
    -- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Nguyen Phan Huynh Thang 
    -- Ngày tạo: 2025-05-18

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: Nguyen Phan Huynh Thang 
    -- Ngày cập nhật: 2025-07-07
    -- Mục đích cập nhật: Tối ưu với cấu trúc bảng mới và sửa lỗi

    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: KHỞI TẠO VÀ GHI LOG
    -- Bước 2: TẠO CÁC BẢNG TẠM CHỨA DỮ LIỆU GỐC ĐÃ QUA XỬ LÝ
    -- Bước 3: TÍNH TOÁN VÀ INSERT DỮ LIỆU BÁO CÁO 1 (fact_backdate_funding_monthly)
    -- Bước 3.5: BACKFILL CÁC funding_id CÒN THIẾU VỚI GIÁ TRỊ 0
    -- Bước 4: TÍNH TOÁN VÀ INSERT DỮ LIỆU BÁO CÁO 2 (fact_backdate_asm_monthly)
    -- Bước 5: CẬP NHẬT LOG VỚI THỜI GIAN KẾT THÚC VÀ TRẠNG THÁI THÀNH CÔNG

    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
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
    -- BƯỚC 2: TẠO CÁC BẢNG TẠM CHỨA DỮ LIỆU GỐC ĐÃ QUA XỬ LÝ
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
            COALESCE(SUM(k.outstanding_principal), 0) AS op_total,
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
        op_total / NULLIF(SUM(op_total) OVER (), 0) AS ratio_op_total,
        op_b1 / NULLIF(SUM(op_b1) OVER (), 0) AS ratio_op_b1,
        op_b2 / NULLIF(SUM(op_b2) OVER (), 0) AS ratio_op_b2,
        op_b2_5 / NULLIF(SUM(op_b2_5) OVER (), 0) AS ratio_op_b2_5,
        psdn_count::NUMERIC / NULLIF(SUM(psdn_count) OVER (), 0) AS ratio_psdn,
        (npl + wo_lk) / NULLIF(total_outstanding + wo_lk, 0) AS npl_truoc_wo_luy_ke
    FROM kpi_aggregated;
    
    -- Xác định cột tháng động để đếm số lượng nhân sự
    SELECT
        CASE v_month_num
            WHEN 1 THEN 'ltn_jan' WHEN 2 THEN 'ltn_feb' WHEN 3 THEN 'ltn_mar' WHEN 4 THEN 'ltn_apr'
            WHEN 5 THEN 'ltn_may' WHEN 6 THEN 'ltn_jun' WHEN 7 THEN 'ltn_july' WHEN 8 THEN 'ltn_aug'
            WHEN 9 THEN 'ltn_sep' WHEN 10 THEN 'ltn_oct' WHEN 11 THEN 'ltn_nov' WHEN 12 THEN 'ltn_dec'
        END,
        CASE v_month_num
            WHEN 1 THEN 'psdn_jan' WHEN 2 THEN 'psdn_feb' WHEN 3 THEN 'psdn_mar' WHEN 4 THEN 'psdn_apr'
            WHEN 5 THEN 'psdn_may' WHEN 6 THEN 'psdn_jun' WHEN 7 THEN 'psdn_july' WHEN 8 THEN 'psdn_aug'
            WHEN 9 THEN 'psdn_sep' WHEN 10 THEN 'psdn_oct' WHEN 11 THEN 'psdn_nov' WHEN 12 THEN 'psdn_dec'
        END,
        CASE v_month_num
            WHEN 1 THEN 'approved_rate_jan' WHEN 2 THEN 'approved_rate_feb' WHEN 3 THEN 'approved_rate_mar'
            WHEN 4 THEN 'approved_rate_apr' WHEN 5 THEN 'approved_rate_may' WHEN 6 THEN 'approved_rate_jun'
            WHEN 7 THEN 'approved_rate_july' WHEN 8 THEN 'approved_rate_aug' WHEN 9 THEN 'approved_rate_sep'
            WHEN 10 THEN 'approved_rate_oct' WHEN 11 THEN 'approved_rate_nov' WHEN 12 THEN 'approved_rate_dec'
        END
    INTO v_ltn_column, v_psdn_column, v_approved_rate_column;

    -- Bảng tạm 2: Lấy số lượng nhân sự SM theo khu vực (SỬ DỤNG DYNAMIC SQL)
    EXECUTE format('
        CREATE TEMP TABLE tmp_sm_counts ON COMMIT DROP AS
        SELECT
            am.area_cde,
            COUNT(k.%I) AS sm_count
        FROM kpi_asm_data k
        JOIN area_mapping am ON k.area_name = am.area_name
        WHERE k.%I IS NOT NULL
        GROUP BY am.area_cde;
    ', v_ltn_column, v_ltn_column);

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
            COALESCE(SUM(CASE WHEN account_code IN (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101) THEN total_amount END), 0) AS f20_amount,
            COALESCE(SUM(CASE WHEN account_code IN (816000000001,816000000002,816000000003) THEN total_amount END), 0) AS f21_amount,
            COALESCE(SUM(CASE WHEN account_code IN (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001) THEN total_amount END), 0) AS f22_amount,
            COALESCE(SUM(CASE WHEN CAST(account_code AS VARCHAR) LIKE '85%' THEN total_amount END), 0) AS f25_amount,
            COALESCE(SUM(CASE WHEN CAST(account_code AS VARCHAR) LIKE '86%' THEN total_amount END), 0) AS f26_amount,
            COALESCE(SUM(CASE WHEN CAST(account_code AS VARCHAR) LIKE '87%' THEN total_amount END), 0) AS f27_amount,
            COALESCE(SUM(CASE WHEN account_code IN (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001, 882200050101, 882200020101, 882200060001,790000050101, 882200030101) THEN total_amount END), 0) AS f28_amount
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
            (SELECT f20_amount FROM head_distributable) * r.ratio_op_total + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code IN (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)), 0) AS f20_amount,
            (SELECT f21_amount FROM head_distributable) * r.ratio_op_total + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code IN (816000000001,816000000002,816000000003)), 0) AS f21_amount,
            (SELECT f22_amount FROM head_distributable) * r.ratio_op_total + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code IN (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)), 0) AS f22_amount,
            (SELECT f25_amount FROM head_distributable) * (s.sm_count::numeric / NULLIF((SELECT SUM(sm_count) FROM tmp_sm_counts), 0)) + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND CAST(t.account_code AS VARCHAR) LIKE '85%'), 0) AS f25_amount,
            (SELECT f26_amount FROM head_distributable) * (s.sm_count::numeric / NULLIF((SELECT SUM(sm_count) FROM tmp_sm_counts), 0)) + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND CAST(t.account_code AS VARCHAR) LIKE '86%'), 0) AS f26_amount,
            (SELECT f27_amount FROM head_distributable) * (s.sm_count::numeric / NULLIF((SELECT SUM(sm_count) FROM tmp_sm_counts), 0)) + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND CAST(t.account_code AS VARCHAR) LIKE '87%'), 0) AS f27_amount,
            (SELECT f28_amount FROM head_distributable) * r.ratio_op_b2_5 + COALESCE((SELECT SUM(t.total_amount) FROM tmp_txn_sums t WHERE t.area_code = r.area_cde AND t.account_code IN (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)), 0) AS f28_amount
        FROM tmp_kpi_ratios r
        JOIN tmp_sm_counts s ON r.area_cde = s.area_cde
    ),
    -- 3. Gộp số liệu gốc của Hội sở và Khu vực
    base_metrics AS (
        SELECT area_cde, f9_amount, f10_amount, f11_amount, f12_amount, f13_amount, f20_amount, f21_amount, f22_amount, f25_amount, f26_amount, f27_amount, f28_amount FROM regional_final_amounts
        UNION ALL
        SELECT area_cde, f9_amount, f10_amount, f11_amount, f12_amount, f13_amount, f20_amount, f21_amount, f22_amount, f25_amount, f26_amount, f27_amount, f28_amount FROM head_distributable
    ),
    -- 4. Tính toán tất cả các chỉ số tổng hợp và tài chính
    final_calcs AS (
        WITH 
        regional_direct_card_income AS (
            SELECT
                area_code,
                SUM(total_amount) as v_tnt_regional
            FROM tmp_txn_sums
            WHERE area_code != 'A' AND account_code IN (702000030002, 702000030001, 702000030102, 702000030012, 702000030112, 716000000001, 719000030002, 719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
            GROUP BY area_code
        ),
        totals_for_ratio AS (
            SELECT
                (SELECT SUM(total_amount) FROM tmp_txn_sums WHERE account_code IN ('702000040001','702000040002','703000000001','703000000002','703000000003','703000000004', '721000000041','721000000037','721000000039','721000000013','721000000014','721000000036','723000000014', '723000000037','821000000014','821000000037','821000000039','821000000041','821000000013','821000000036', '823000000014','823000000037','741031000001','741031000002','841000000001','841000000005','841000000004', '701000000001','701000000002','701037000001','701037000002','701000000101')) AS v_doanh_thu_nguon_von_toan_hang,
                (SELECT SUM(f9_amount + f10_amount) FROM regional_final_amounts) AS v_lai_tvth,
                (SELECT SUM(total_amount) FROM tmp_txn_sums WHERE area_code = 'A' AND account_code IN (801000000001, 802000000001)) AS v_cpvtt2_head,
                (SELECT SUM(total_amount) FROM tmp_txn_sums WHERE area_code = 'A' AND account_code = 803000000001) AS v_cpcctg_head
        ),
        -- Tính các chi phí vốn
        metrics_with_costs AS (
            SELECT
                bm.*,
                CASE 
                    WHEN bm.area_cde = 'A' THEN (SELECT v_cpvtt2_head FROM totals_for_ratio)
                    ELSE COALESCE((SELECT v_cpvtt2_head FROM totals_for_ratio) * rdci.v_tnt_regional / NULLIF((SELECT v_doanh_thu_nguon_von_toan_hang + v_lai_tvth FROM totals_for_ratio), 0), 0)
                END AS f15_amount,
                CASE
                    WHEN bm.area_cde = 'A' THEN (SELECT v_cpcctg_head FROM totals_for_ratio)
                    ELSE COALESCE((SELECT v_cpcctg_head FROM totals_for_ratio) * rdci.v_tnt_regional / NULLIF((SELECT v_doanh_thu_nguon_von_toan_hang + v_lai_tvth FROM totals_for_ratio), 0), 0)
                END AS f17_amount
            FROM base_metrics bm
            LEFT JOIN regional_direct_card_income rdci ON bm.area_cde = rdci.area_code
        ),
        -- Tính các chỉ số tổng hợp
        composite_metrics AS (
            SELECT
                mc.*,
                (mc.f9_amount + mc.f10_amount + mc.f11_amount + mc.f12_amount + mc.f13_amount) AS f4_amount,
                (mc.f15_amount + mc.f17_amount) AS f5_amount,
                (mc.f20_amount + mc.f21_amount + mc.f22_amount) AS f6_amount,
                (mc.f25_amount + mc.f26_amount + mc.f27_amount) AS f8_amount
            FROM metrics_with_costs mc
        )
        -- Tính các chỉ số cấp cao hơn và các tỷ lệ tài chính
        SELECT
            cm.*,
            (cm.f4_amount + cm.f5_amount + cm.f6_amount) AS f7_amount,
            (cm.f4_amount + cm.f5_amount + cm.f6_amount + cm.f8_amount + cm.f28_amount) AS f1_amount,
            COALESCE((cm.f8_amount / NULLIF((cm.f4_amount + cm.f5_amount + cm.f6_amount), 0)) * 100 * (-1), 0) AS f29_cir,
            COALESCE(((cm.f4_amount + cm.f5_amount + cm.f6_amount + cm.f8_amount + cm.f28_amount) / NULLIF(cm.f4_amount + cm.f20_amount, 0)) * 100, 0) AS f30_margin,
            COALESCE(-((cm.f4_amount + cm.f5_amount + cm.f6_amount + cm.f8_amount + cm.f28_amount) / NULLIF(cm.f5_amount, 0)) * 100, 0) AS f31_hs_von,
            COALESCE((cm.f4_amount + cm.f5_amount + cm.f6_amount + cm.f8_amount + cm.f28_amount) / NULLIF(
                CASE
                    WHEN cm.area_cde = 'A' THEN (SELECT SUM(sm_count) FROM tmp_sm_counts)
                    ELSE s.sm_count
                END, 0), 0) AS f32_hsbqns
        FROM composite_metrics cm
        LEFT JOIN tmp_sm_counts s ON cm.area_cde = s.area_cde
    ),
    -- 5. Unpivot tất cả dữ liệu thành dạng dòng để INSERT
    final_data AS (
        SELECT area_cde, 1 AS funding_id, f1_amount AS amount FROM final_calcs UNION ALL
        SELECT area_cde, 4, f4_amount FROM final_calcs UNION ALL
        SELECT area_cde, 5, f5_amount FROM final_calcs UNION ALL
        SELECT area_cde, 6, f6_amount FROM final_calcs UNION ALL
        SELECT area_cde, 7, f7_amount FROM final_calcs UNION ALL
        SELECT area_cde, 8, f8_amount FROM final_calcs UNION ALL
        SELECT area_cde, 9, f9_amount FROM final_calcs UNION ALL
        SELECT area_cde, 10, f10_amount FROM final_calcs UNION ALL
        SELECT area_cde, 11, f11_amount FROM final_calcs UNION ALL
        SELECT area_cde, 12, f12_amount FROM final_calcs UNION ALL
        SELECT area_cde, 13, f13_amount FROM final_calcs UNION ALL
        SELECT area_cde, 15, f15_amount FROM final_calcs UNION ALL
        SELECT area_cde, 17, f17_amount FROM final_calcs UNION ALL
        SELECT area_cde, 20, f20_amount FROM final_calcs UNION ALL
        SELECT area_cde, 21, f21_amount FROM final_calcs UNION ALL
        SELECT area_cde, 22, f22_amount FROM final_calcs UNION ALL
        SELECT area_cde, 25, f25_amount FROM final_calcs UNION ALL
        SELECT area_cde, 26, f26_amount FROM final_calcs UNION ALL
        SELECT area_cde, 27, f27_amount FROM final_calcs UNION ALL
        SELECT area_cde, 28, f28_amount FROM final_calcs UNION ALL
        SELECT area_cde, 29, f29_cir FROM final_calcs UNION ALL
        SELECT area_cde, 30, f30_margin FROM final_calcs UNION ALL
        SELECT area_cde, 31, f31_hs_von FROM final_calcs UNION ALL
        SELECT area_cde, 32, f32_hsbqns FROM final_calcs UNION ALL
        -- Bổ sung funding_id = 2 (Số lượng nhân sự)
        SELECT area_cde, 2 AS funding_id, sm_count::NUMERIC AS amount FROM tmp_sm_counts
        UNION ALL
        SELECT 'A' AS area_cde, 2 AS funding_id, SUM(sm_count)::NUMERIC AS amount FROM tmp_sm_counts
    )
    -- 6. Insert dữ liệu cuối cùng vào bảng fact
    INSERT INTO fact_backdate_funding_monthly(funding_id, month_key, area_code, amount)
    SELECT funding_id, p_rp_month, area_cde, amount FROM final_data;

    -- BƯỚC 3.5: Backfill các funding_id còn thiếu với giá trị 0
    WITH all_possible_entries AS (
        SELECT
            d.funding_id,
            a.area_cde AS area_code
        FROM dim_funding_structure d
        CROSS JOIN (SELECT area_cde FROM area_mapping) a
    )
    INSERT INTO fact_backdate_funding_monthly(funding_id, month_key, area_code, amount)
    SELECT
        ape.funding_id,
        p_rp_month,
        ape.area_code,
        0
    FROM all_possible_entries ape
    WHERE NOT EXISTS (
        SELECT 1
        FROM fact_backdate_funding_monthly f
        WHERE f.month_key = p_rp_month
          AND f.funding_id = ape.funding_id
          AND f.area_code = ape.area_code
    );

    -- ---------------------
    -- BƯỚC 4: TÍNH TOÁN DỮ LIỆU BÁO CÁO 2 (fact_backdate_asm_monthly)
    -- ---------------------
    WITH 
    -- CTE 1: Tính các chỉ số trung bình lũy kế
    asm_avg_calcs AS (
        SELECT
            k.email,
            (CASE v_month_num
                WHEN 1 THEN COALESCE(k.ltn_jan, 0)
                WHEN 2 THEN COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0)
                WHEN 3 THEN COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0) + COALESCE(k.ltn_mar, 0)
                WHEN 4 THEN COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0) + COALESCE(k.ltn_mar, 0) + COALESCE(k.ltn_apr, 0)
                WHEN 5 THEN COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0) + COALESCE(k.ltn_mar, 0) + COALESCE(k.ltn_apr, 0) + COALESCE(k.ltn_may, 0)
                -- Thêm các tháng còn lại nếu cần
                ELSE 0
            END)::NUMERIC / v_month_num AS ltn_avg,
            (CASE v_month_num
                WHEN 1 THEN COALESCE(k.psdn_jan, 0)
                WHEN 2 THEN COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0)
                WHEN 3 THEN COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0) + COALESCE(k.psdn_mar, 0)
                WHEN 4 THEN COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0) + COALESCE(k.psdn_mar, 0) + COALESCE(k.psdn_apr, 0)
                WHEN 5 THEN COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0) + COALESCE(k.psdn_mar, 0) + COALESCE(k.psdn_apr, 0) + COALESCE(k.psdn_may, 0)
                -- Thêm các tháng còn lại nếu cần
                ELSE 0
            END)::NUMERIC / v_month_num AS psdn_avg,
            (CASE v_month_num
                WHEN 1 THEN COALESCE(k.approved_rate_jan, 0)
                WHEN 2 THEN COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0)
                WHEN 3 THEN COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0) + COALESCE(k.approved_rate_mar, 0)
                WHEN 4 THEN COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0) + COALESCE(k.approved_rate_mar, 0) + COALESCE(k.approved_rate_apr, 0)
                WHEN 5 THEN COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0) + COALESCE(k.approved_rate_mar, 0) + COALESCE(k.approved_rate_apr, 0) + COALESCE(k.approved_rate_may, 0)
                -- Thêm các tháng còn lại nếu cần
                ELSE 0
            END)::NUMERIC / v_month_num AS approval_rate_avg
        FROM kpi_asm_data k
        WHERE 
            CASE
                WHEN v_ltn_column = 'ltn_jan' THEN k.ltn_jan IS NOT NULL
                WHEN v_ltn_column = 'ltn_feb' THEN k.ltn_feb IS NOT NULL
                when v_ltn_column = 'ltn_mar' THEN k.ltn_mar IS NOT NULL
                WHEN v_ltn_column = 'ltn_apr' THEN k.ltn_apr IS NOT NULL
                WHEN v_ltn_column = 'ltn_may' THEN k.ltn_may IS NOT NULL
                -- Thêm các tháng còn lại
                ELSE FALSE
            END
    ),
    -- CTE 2: Tập hợp tất cả dữ liệu cần thiết cho báo cáo ASM
    asm_full_data AS (
        SELECT
            am.area_cde,
            k.area_name,
            k.email,
            avg.ltn_avg,
            avg.psdn_avg,
            avg.approval_rate_avg,
            ratios.npl_truoc_wo_luy_ke,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 29 AND sub.area_code = am.area_cde) AS cir,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 30 AND sub.area_code = am.area_cde) AS margin,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 31 AND sub.area_code = am.area_cde) AS hs_von,
            (SELECT amount FROM fact_backdate_funding_monthly sub WHERE sub.month_key = p_rp_month AND sub.funding_id = 32 AND sub.area_code = am.area_cde) AS hsbq_nhan_su
        FROM kpi_asm_data k
        JOIN area_mapping am ON k.area_name = am.area_name
        JOIN asm_avg_calcs avg ON k.email = avg.email
        LEFT JOIN tmp_kpi_ratios ratios ON am.area_cde = ratios.area_cde
    ),
    -- CTE 3: Xếp hạng các chỉ số
    ranked_data AS (
        SELECT
            *,
            RANK() OVER (ORDER BY ltn_avg DESC) as rank_ltn_avg,
            RANK() OVER (ORDER BY psdn_avg DESC) as rank_psdn_avg,
            RANK() OVER (ORDER BY approval_rate_avg DESC) as rank_approval_rate_avg,
            RANK() OVER (ORDER BY npl_truoc_wo_luy_ke ASC) as rank_npl_truoc_wo_luy_ke,
            DENSE_RANK() OVER (ORDER BY cir ASC) as rank_cir,
            DENSE_RANK() OVER (ORDER BY margin DESC) as rank_margin,
            DENSE_RANK() OVER (ORDER BY hs_von DESC) as rank_hs_von,
            DENSE_RANK() OVER (ORDER BY hsbq_nhan_su DESC) as rank_hsbq_nhan_su
        FROM asm_full_data
    ),
    -- CTE 4: Tính điểm
    final_scores AS (
        SELECT
            *,
            (rank_ltn_avg + rank_psdn_avg + rank_approval_rate_avg + rank_npl_truoc_wo_luy_ke) AS diem_quy_mo,
            (rank_cir + rank_margin + rank_hs_von + rank_hsbq_nhan_su) as diem_fin
        FROM ranked_data
    )
    -- CTE 5: Xếp hạng cuối cùng
    INSERT INTO fact_backdate_asm_monthly(
        month_key, area_cde, area_name, email, tongdiem, rank_final, 
        ltn_avg, rank_ltn_avg, psdn_avg, rank_psdn_avg, approval_rate_avg, rank_approval_rate_avg,
        npl_truoc_wo_luy_ke, rank_npl_truoc_wo_luy_ke, diem_quy_mo, rank_ptkd,
        cir, rank_cir, margin, rank_margin, hs_von, rank_hs_von, hsbq_nhan_su, rank_hsbq_nhan_su,
        diem_fin, rank_fin
    )
    SELECT
        p_rp_month, area_cde, area_name, email,
        diem_quy_mo + diem_fin AS tongdiem,
        RANK() OVER (ORDER BY (diem_quy_mo + diem_fin) ASC) as rank_final,
        ltn_avg, rank_ltn_avg, psdn_avg, rank_psdn_avg, approval_rate_avg, rank_approval_rate_avg,
        npl_truoc_wo_luy_ke, rank_npl_truoc_wo_luy_ke,
        diem_quy_mo,
        RANK() OVER (ORDER BY diem_quy_mo ASC) as rank_ptkd,
        cir, rank_cir, margin, rank_margin, hs_von, rank_hs_von, hsbq_nhan_su, rank_hsbq_nhan_su,
        diem_fin,
        RANK() OVER (ORDER BY diem_fin ASC) as rank_fin
    FROM final_scores;

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
call prc_generate_summary_reports_monthly(202304);

--------------------------------------------------------------------------------
-- HÀM XUẤT BÁO CÁO TỔNG HỢP (PIVOT)
--------------------------------------------------------------------------------
-- Xóa hàm cũ nếu tồn tại để tránh lỗi
DROP FUNCTION IF EXISTS fn_get_monthly_summary_report(BIGINT);

-- Tạo hàm mới để xuất báo cáo theo định dạng mong muốn
CREATE OR REPLACE FUNCTION fn_get_monthly_summary_report(p_rp_month BIGINT)
-- Định nghĩa các cột trả về của hàm, khớp với format trong hình
RETURNS TABLE (
    "funding_name" TEXT,
    "Head" NUMERIC,
    "Miền Bắc" NUMERIC,
    "Miền Nam" NUMERIC,
    "Miền Trung" NUMERIC,
    "Total" NUMERIC,
    "Đông Bắc Bộ" NUMERIC,
    "Tây Bắc Bộ" NUMERIC,
    "ĐB Sông Hồng" NUMERIC,
    "Bắc Trung Bộ" NUMERIC,
    "Nam Trung Bộ" NUMERIC,
    "Tây Nam Bộ" NUMERIC,
    "Đông Nam Bộ" NUMERIC,
    "TOTAL" NUMERIC
)
AS $$
BEGIN
    -- Câu lệnh RETURN QUERY sẽ thực thi truy vấn và trả về kết quả
    RETURN QUERY
    WITH pivoted_data AS (
        -- Bước 1: Xoay dữ liệu từ dạng hàng sang cột
        -- Join với dim_funding_structure để lấy tên và thứ tự chỉ tiêu
        -- Dùng SUM(CASE...) để gán giá trị 'amount' vào đúng cột 'area_code'
        SELECT
            d.funding_id,
            d.funding_name,
            d.sortorder,
            -- Cột HEAD
            SUM(CASE WHEN f.area_code = 'A' THEN f.amount ELSE 0 END) AS head_amount,
            -- Các cột cho từng khu vực kinh doanh
            SUM(CASE WHEN f.area_code = 'B' THEN f.amount ELSE 0 END) AS dong_bac_bo,
            SUM(CASE WHEN f.area_code = 'C' THEN f.amount ELSE 0 END) AS tay_bac_bo,
            SUM(CASE WHEN f.area_code = 'D' THEN f.amount ELSE 0 END) AS db_song_hong,
            SUM(CASE WHEN f.area_code = 'E' THEN f.amount ELSE 0 END) AS bac_trung_bo,
            SUM(CASE WHEN f.area_code = 'F' THEN f.amount ELSE 0 END) AS nam_trung_bo,
            SUM(CASE WHEN f.area_code = 'G' THEN f.amount ELSE 0 END) AS tay_nam_bo,
            SUM(CASE WHEN f.area_code = 'H' THEN f.amount ELSE 0 END) AS dong_nam_bo
        FROM fact_backdate_funding_monthly f
        JOIN dim_funding_structure d ON f.funding_id = d.funding_id
        WHERE f.month_key = p_rp_month
        GROUP BY d.funding_id, d.funding_name, d.sortorder
    )
    -- Bước 2: Tính các cột tổng hợp và định dạng đầu ra cuối cùng
    SELECT
        p.funding_name::TEXT,
        p.head_amount,
        NULL::NUMERIC, -- Cột Miền Bắc, để trống như trong hình
        NULL::NUMERIC, -- Cột Miền Nam, để trống như trong hình
        NULL::NUMERIC, -- Cột Miền Trung, để trống như trong hình
        p.head_amount AS total_head, -- Cột Total (trái) bằng cột HEAD
        p.dong_bac_bo,
        p.tay_bac_bo,
        p.db_song_hong,
        p.bac_trung_bo,
        p.nam_trung_bo,
        p.tay_nam_bo,
        p.dong_nam_bo,
        -- Cột Total KVML (phải) là tổng của tất cả các khu vực
        (p.dong_bac_bo + p.tay_bac_bo + p.db_song_hong + p.bac_trung_bo + p.nam_trung_bo + p.tay_nam_bo + p.dong_nam_bo) AS total_kvml
    FROM pivoted_data p
    ORDER BY p.sortorder; -- Sắp xếp các chỉ tiêu theo đúng thứ tự
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fn_get_monthly_summary_report(202304);

-- HÀM XUẤT BÁO CÁO XẾP HẠNG ASM
--------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_get_asm_ranking_report(BIGINT);

CREATE OR REPLACE FUNCTION fn_get_asm_ranking_report(p_rp_month BIGINT)
RETURNS TABLE (
    month_key int8,
    area_cde varchar(200),
    area_name varchar(200),
    email varchar(200),
    "Tổng điểm" numeric,
    rank_final int8,
    ltn_avg numeric,
    rank_ltn_avg int8,
    psdn_avg numeric,
    rank_psdn_avg int8,
    approval_rate_avg numeric,
    rank_approval_rate_avg int8,
    npl_truoc_wo_luy_ke numeric,
    rank_npl_truoc_wo_luy_ke int8,
    "Điểm Quy Mô" numeric,
    rank_ptkd int8,
    cir numeric,
    rank_cir int8,
    margin numeric,
    rank_margin int8,
    hs_von numeric,
    rank_hs_von int8,
    hsbq_nhan_su numeric,
    rank_hsbq_nhan_su int8,
    "Điểm FIN" int8,
    rank_fin int8
)
AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM fact_backdate_asm_monthly f
    WHERE f.month_key = p_rp_month
    ORDER BY f.rank_final;
END;
$$ LANGUAGE plpgsql;

select * from fn_get_asm_ranking_report(202302);