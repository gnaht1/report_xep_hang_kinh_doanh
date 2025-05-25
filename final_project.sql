-- 2. Đưa dữ liệu input vào CSDL đồng thời viết script check để kiểm tra việc import dữ liệu vào đã chính xác hay chưa 
-- ( khâu này cực kỳ quan trọng ).
select count(*) from fact_kpi_month ; -- 2932922

select * from fact_kpi_month;

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'fact_kpi_month';

-- 
select count(*) from fact_txn_month  ; -- 63134

select * from fact_txn_month;

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'fact_txn_month';


--3. Đọc hiểu nghiệp vụ trong sheet mô tả và param tổ chức mô hình dữ liệu dim , fact để lưu trữ một cách tối ưu ( tái 
--sử dụng )
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

create table fact_backdate_funding_monthly(
	 funding_id int8 NOT null
	 , tpb_head numeric 
	 , tpb_mienbac numeric
	 , tpb_miennam numeric
	 , tpb_mientrung numeric
	 , tpv_total numeric
	 , kvml_dbb numeric
	 , kvml_tbb numeric
	 , kvml_dbsh numeric
	 , kvml_btb numeric
	 , kvml_ntb numeric
	 , kvml_tnb numeric 
	 , kvml_dnb numeric
	 , kvml_total numeric
	 , month_key int8
);

select * from fact_backdate_funding_monthly ;

-----------------------------------------------------report 2
create table fact_backdate_asm_monthly (
	month_key int8
	, area_cde varchar(200) 
	, area_name varchar(200)
	, email varchar(200)
	, tongdiem numeric
	, rank_final int8
	, ltn_avg numeric
	, rank_ltn_avg int8
	, psdn_avg numeric
	, rank_psdn_avg int8
	, approval_rate_avg numeric
	, rank_approval_rate_avg int8 
	, npl_truoc_wo_luy_ke numeric
	, rank_npl_truoc_wo_luy_ke int8
	, diem_quy_mo numeric
	, rank_ptkd int8
	, cir numeric
	, rank_cir int8
	, margin numeric
	, rank_margin int8
	, hs_von numeric
	, rank_hs_von int8
	, hsbq_nhan_su numeric
	, rank_hsbq_nhan_su int8
	, diem_fin int8 
	, rank_fin int8
);

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


--------------------------------------------------
create table log_tracking(
	id serial primary key 
	, procedure_name text not null 
	, start_time timestamp 
	, end_time timestamp 
	, is_successful bool
	, error_log text 
	, rec_created_dt timestamp default now()
);

---------------------------------- PROCEDURE -------------------------------------------------------------------
create or replace procedure fact_report_monthly_prc(p_rp_month int8)
as $$
declare 
	v_start_log_time timestamp ;
	v_end_log_time timestamp ;
	v_error_msg text;
	v_log_id int8;

	v_start_rp_month int8; 
	-- lai trong han 
	v_laitronghan_head numeric;

	v_dbb_dvml_laitronghan numeric;
	v_tile_pb_dbb_laitronghan numeric;
	v_dbb_pb_laitronghan numeric;

	v_tbb_dvml_laitronghan numeric ;
	v_tile_pb_tbb_laitronghan numeric;
	v_tbb_pb_laitronghan numeric;

	v_dbsh_dvml_laitronghan numeric ;
	v_tile_pb_dbsh_laitronghan numeric;
	v_dbsh_pb_laitronghan numeric;

	v_btb_dvml_laitronghan numeric ;
	v_tile_pb_btb_laitronghan numeric;
	v_btb_pb_laitronghan numeric;

	v_ntb_dvml_laitronghan numeric ;
	v_tile_pb_ntb_laitronghan numeric;
	v_ntb_pb_laitronghan numeric;

	v_tnb_dvml_laitronghan numeric ;
	v_tile_pb_tnb_laitronghan numeric;
	v_tnb_pb_laitronghan numeric;


	v_dnb_dvml_laitronghan numeric ;
	v_tile_pb_dnb_laitronghan numeric;
	v_dnb_pb_laitronghan numeric;

	v_kvml_pb_laitronghan_total numeric;
	-- Lãi quá hạn
	v_laiquahan_head numeric ;

	v_dbb_dvml_laiquahan numeric;
	v_tile_pb_dbb_laiquahan numeric;
	v_dbb_pb_laiquahan numeric;

	v_tbb_dvml_laiquahan numeric;
	v_tile_pb_tbb_laiquahan numeric;
	v_tbb_pb_laiquahan numeric;

	v_dbsh_dvml_laiquahan numeric;
	v_tile_pb_dbsh_laiquahan numeric;
	v_dbsh_pb_laiquahan numeric;

	v_btb_dvml_laiquahan numeric;
	v_tile_pb_btb_laiquahan numeric;
	v_btb_pb_laiquahan numeric;

	v_ntb_dvml_laiquahan numeric;
	v_tile_pb_ntb_laiquahan numeric;
	v_ntb_pb_laiquahan numeric;

	v_tnb_dvml_laiquahan numeric;
	v_tile_pb_tnb_laiquahan numeric;
	v_tnb_pb_laiquahan numeric;

	v_dnb_dvml_laiquahan numeric;
	v_tile_pb_dnb_laiquahan numeric;
	v_dnb_pb_laiquahan numeric;

	v_kvml_pb_laiquahan_total numeric;
	-- phí bảo hiểm
	v_pbh_head numeric;

	v_dbb_dvml_pbh numeric;
	v_tile_pb_dbb_pbh numeric;
	v_dbb_pb_pbh numeric;

	v_tbb_dvml_pbh numeric;
	v_tile_pb_tbb_pbh numeric;
	v_tbb_pb_pbh numeric;

	v_dbsh_dvml_pbh numeric;
	v_tile_pb_dbsh_pbh numeric;
	v_dbsh_pb_pbh numeric;

	v_btb_dvml_pbh numeric;
	v_tile_pb_btb_pbh numeric;
	v_btb_pb_pbh numeric;

	v_ntb_dvml_pbh numeric;
	v_tile_pb_ntb_pbh numeric;
	v_ntb_pb_pbh numeric;

	v_tnb_dvml_pbh numeric;
	v_tile_pb_tnb_pbh numeric;
	v_tnb_pb_pbh numeric;

	v_dnb_dvml_pbh numeric;
	v_tile_pb_dnb_pbh numeric;
	v_dnb_pb_pbh numeric;

	v_kvml_pb_pbh_total numeric;

	-- phi tang han muc
	v_thm_head numeric;

	v_dbb_dvml_thm numeric;
	v_dbb_pb_thm numeric;

	v_tbb_dvml_thm numeric;
	v_tbb_pb_thm numeric;

	v_dbsh_dvml_thm numeric;
	v_dbsh_pb_thm numeric;

	v_btb_dvml_thm numeric;
	v_btb_pb_thm numeric;

	v_ntb_dvml_thm numeric;
	v_ntb_pb_thm numeric;

	v_tnb_dvml_thm numeric;
	v_tnb_pb_thm numeric;

	v_dnb_dvml_thm numeric;
	v_dnb_pb_thm numeric;

	v_kvml_pb_thm_total numeric;

	-- Phí thanh toán chậm, thu từ ngoại bảng, khác…
	v_ttc_head numeric;

	v_dbb_dvml_ttc numeric;
	v_tile_pb_dbb_ttc numeric;
	v_dbb_pb_ttc numeric;

	v_tbb_dvml_ttc numeric;
	v_tile_pb_tbb_ttc numeric;
	v_tbb_pb_ttc numeric;

	v_dbsh_dvml_ttc numeric;
	v_tile_pb_dbsh_ttc numeric;
	v_dbsh_pb_ttc numeric;

	v_btb_dvml_ttc numeric;
	v_tile_pb_btb_ttc numeric;
	v_btb_pb_ttc numeric;

	v_ntb_dvml_ttc numeric;
	v_tile_pb_ntb_ttc numeric;
	v_ntb_pb_ttc numeric;

	v_tnb_dvml_ttc numeric;
	v_tile_pb_tnb_ttc numeric;
	v_tnb_pb_ttc numeric;

	v_dnb_dvml_ttc numeric;
	v_tile_pb_dnb_ttc numeric;
	v_dnb_pb_ttc numeric;

	v_kvml_pb_ttc_total numeric;

	-- Thu nhập từ hoạt động thẻ
	v_tnt_head numeric;
	v_tnt_dbb numeric;
	v_tnt_tbb numeric;
	v_tnt_dbsh numeric;
	v_tnt_btb numeric;
	v_tnt_ntb numeric;
	v_tnt_tnb numeric;
	v_tnt_dnb numeric;
	v_tnt_total numeric;

	-- CP vốn TT 2--
	v_cpvtt2_head numeric;
	v_doanh_thu_nguon_von_toan_hang numeric;
	v_lai_tvth numeric;
	v_dbb_pb_tt2 numeric;
	v_tbb_pb_tt2 numeric;
	v_dbsh_pb_tt2 numeric;
	v_btb_pb_tt2 numeric;
	v_ntb_pb_tt2 numeric;
	v_tnb_pb_tt2 numeric;
	v_dnb_pb_tt2 numeric;
	v_kvml_pb_tt2_total numeric;

	-- CP von CCTG
	v_cpcctg_head numeric;
	v_cpcctg_dbb numeric;
	v_cpcctg_tbb numeric;
	v_cpcctg_dbsh numeric;
	v_cpcctg_btb numeric;
	v_cpcctg_ntb numeric;
	v_cpcctg_tnb numeric;
	v_cpcctg_dnb numeric;
	v_cpcctg_total numeric;

	-- Chi phí thuần KDV
	v_cp_kdv_head numeric;
	v_cp_kdv_dbb numeric;
	v_cp_kdv_tbb numeric;
	v_cp_kdv_dbsh numeric;
	v_cp_kdv_btb numeric;
	v_cp_kdv_ntb numeric;
	v_cp_kdv_tnb numeric;
	v_cp_kdv_dnb numeric;
	v_cp_kdv_total numeric;

	-- DT kinh doanh
	v_dt_kd_head numeric;

	v_dbb_kvml_dt_kd numeric;
	v_tile_pb_dbb_dt_kd numeric;
	v_dbb_pb_dt_kd numeric;

	v_tbb_kvml_dt_kd numeric;
	v_tile_pb_tbb_dt_kd numeric;
	v_tbb_pb_dt_kd numeric;

	v_dbsh_kvml_dt_kd numeric;
	v_tile_pb_dbsh_dt_kd numeric;
	v_dbsh_pb_dt_kd numeric;

	v_btb_kvml_dt_kd numeric;
	v_tile_pb_btb_dt_kd numeric;
	v_btb_pb_dt_kd numeric;

	v_ntb_kvml_dt_kd numeric;
	v_tile_pb_ntb_dt_kd numeric;
	v_ntb_pb_dt_kd numeric;

	v_tnb_kvml_dt_kd numeric;
	v_tile_pb_tnb_dt_kd numeric;
	v_tnb_pb_dt_kd numeric;

	v_dnb_kvml_dt_kd numeric;
	v_tile_pb_dnb_dt_kd numeric;
	v_dnb_pb_dt_kd numeric;

	v_kvml_pb_dt_kd_total numeric;

	-- CP hoa hồng
	v_cp_hh_head numeric;

	v_dbb_kvml_cp_hh numeric;
	v_dbb_pb_cp_hh numeric;

	v_tbb_kvml_cp_hh numeric;
	v_tbb_pb_cp_hh numeric;

	v_dbsh_kvml_cp_hh numeric;
	v_dbsh_pb_cp_hh numeric;

	v_btb_kvml_cp_hh numeric;
	v_btb_pb_cp_hh numeric;

	v_ntb_kvml_cp_hh numeric;
	v_ntb_pb_cp_hh numeric;

	v_tnb_kvml_cp_hh numeric;
	v_tnb_pb_cp_hh numeric;

	v_dnb_kvml_cp_hh numeric;
	v_dnb_pb_cp_hh numeric;

	v_kvml_pb_cp_hh_total numeric;

	-- CP thuần KD khác 
	v_cp_kdk_head numeric;

	v_dbb_kvml_cp_kdk numeric;
	v_dbb_pb_cp_kdk numeric;

	v_tbb_kvml_cp_kdk numeric;
	v_tbb_pb_cp_kdk numeric;

	v_dbsh_kvml_cp_kdk numeric;
	v_dbsh_pb_cp_kdk numeric;

	v_btb_kvml_cp_kdk numeric;
	v_btb_pb_cp_kdk numeric;

	v_ntb_kvml_cp_kdk numeric;
	v_ntb_pb_cp_kdk numeric;

	v_tnb_kvml_cp_kdk numeric;
	v_tnb_pb_cp_kdk numeric;

	v_dnb_kvml_cp_kdk numeric;
	v_dnb_pb_cp_kdk numeric;

	v_kvml_pb_cp_kdk_total numeric;

	-- Chi phí thuần hoạt động khác
	v_cp_hdk_head numeric;
	v_cp_hdk_dbb numeric;
	v_cp_hdk_tbb numeric;
	v_cp_hdk_dbsh numeric;
	v_cp_hdk_btb numeric;
	v_cp_hdk_ntb numeric;
	v_cp_hdk_tnb numeric;
	v_cp_hdk_dnb numeric;
	v_cp_hdk_total numeric;

	-- Tổng thu nhập hoạt động
	v_tong_tnhd_head numeric;
	v_tong_tnhd_dbb numeric;
	v_tong_tnhd_tbb numeric;
	v_tong_tnhd_dbsh numeric;
	v_tong_tnhd_btb numeric;
	v_tong_tnhd_ntb numeric;
	v_tong_tnhd_tnb numeric;
	v_tong_tnhd_dnb numeric;
	v_tong_tnhd_total numeric;

	-- 2. Số lượng nhân sự ( Sale Manager )
	v_count_sm_total numeric;
	v_count_sm_dbb numeric;
	v_count_sm_tbb numeric;
	v_count_sm_dbsh numeric;
	v_count_sm_btb numeric;
	v_count_sm_ntb numeric;
	v_count_sm_tnb numeric;
	v_count_sm_dnb numeric;

	-- CP nhân viên
	v_cp_nv_head numeric;

	v_dbb_kvml_cp_nv numeric;
	v_tile_pb_dbb_cp_nv numeric;
	v_dbb_pb_cp_nv numeric;

	v_tbb_kvml_cp_nv numeric;
	v_tile_pb_tbb_cp_nv numeric;
	v_tbb_pb_cp_nv numeric;

	v_dbsh_kvml_cp_nv numeric;
	v_tile_pb_dbsh_cp_nv numeric;
	v_dbsh_pb_cp_nv numeric;

	v_btb_kvml_cp_nv numeric;
	v_tile_pb_btb_cp_nv numeric;
	v_btb_pb_cp_nv numeric;

	v_ntb_kvml_cp_nv numeric;
	v_tile_pb_ntb_cp_nv numeric;
	v_ntb_pb_cp_nv numeric;

	v_tnb_kvml_cp_nv numeric;
	v_tile_pb_tnb_cp_nv numeric;
	v_tnb_pb_cp_nv numeric;

	v_dnb_kvml_cp_nv numeric;
	v_tile_pb_dnb_cp_nv numeric;
	v_dnb_pb_cp_nv numeric;

	v_kvml_pb_cp_nv_total numeric;

	-- CP quản lý
	v_cp_ql_head numeric;

	v_dbb_kvml_cp_ql numeric;
	v_dbb_pb_cp_ql numeric;

	v_tbb_kvml_cp_ql numeric;
	v_tbb_pb_cp_ql numeric;

	v_dbsh_kvml_cp_ql numeric;
	v_dbsh_pb_cp_ql numeric;

	v_btb_kvml_cp_ql numeric;
	v_btb_pb_cp_ql numeric;

	v_ntb_kvml_cp_ql numeric;
	v_ntb_pb_cp_ql numeric;

	v_tnb_kvml_cp_ql numeric;
	v_tnb_pb_cp_ql numeric;

	v_dnb_kvml_cp_ql numeric;
	v_dnb_pb_cp_ql numeric;

	v_kvml_pb_cp_ql_total numeric;

	-- CP tài sản
	v_cp_ts_head numeric;

	v_dbb_kvml_cp_ts numeric;
	v_dbb_pb_cp_ts numeric;

	v_tbb_kvml_cp_ts numeric;
	v_tbb_pb_cp_ts numeric;

	v_dbsh_kvml_cp_ts numeric;
	v_dbsh_pb_cp_ts numeric;

	v_btb_kvml_cp_ts numeric;
	v_btb_pb_cp_ts numeric;

	v_ntb_kvml_cp_ts numeric;
	v_ntb_pb_cp_ts numeric;

	v_tnb_kvml_cp_ts numeric;
	v_tnb_pb_cp_ts numeric;

	v_dnb_kvml_cp_ts numeric;
	v_dnb_pb_cp_ts numeric;

	v_kvml_pb_cp_ts_total numeric;

	-- Tổng chi phí hoạt động
	v_tong_cp_hd_head numeric;
	v_tong_cp_hd_dbb numeric;
	v_tong_cp_hd_tbb numeric;
	v_tong_cp_hd_dbsh numeric;
	v_tong_cp_hd_btb numeric;
	v_tong_cp_hd_ntb numeric;
	v_tong_cp_hd_tnb numeric;
	v_tong_cp_hd_dnb numeric;
	v_tong_cp_hd_total_kvml numeric;

	-- Chi phí dự phòng
	v_cp_dp_head numeric;

	v_dbb_kvml_cp_dp numeric;
	v_dbb_pb_cp_dp numeric;

	v_tbb_kvml_cp_dp numeric;
	v_tbb_pb_cp_dp numeric;

	v_dbsh_kvml_cp_dp numeric;
	v_dbsh_pb_cp_dp numeric;

	v_btb_kvml_cp_dp numeric;
	v_btb_pb_cp_dp numeric;

	v_ntb_kvml_cp_dp numeric;
	v_ntb_pb_cp_dp numeric;

	v_tnb_kvml_cp_dp numeric;
	v_tnb_pb_cp_dp numeric;

	v_dnb_kvml_cp_dp numeric;
	v_dnb_pb_cp_dp numeric;

	v_kvml_pb_cp_dp_total numeric;

	-- 1. Lợi nhuận trước thuế
	v_lntt_head numeric;
	v_lntt_dbb numeric;
	v_lntt_tbb numeric;
	v_lntt_dbsh numeric;
	v_lntt_btb numeric;
	v_lntt_ntb numeric;
	v_lntt_tnb numeric;
	v_lntt_dnb numeric;
	v_lntt_total_kvml numeric;

	-- CIR (%)
	v_cir_head numeric;
	v_cir_dbb numeric;
	v_cir_tbb numeric;
	v_cir_dbsh numeric;
	v_cir_btb numeric;
	v_cir_ntb numeric;
	v_cir_tnb numeric;
	v_cir_dnb numeric;
	v_cir_total_kvml numeric;

	-- Margin (%)
	v_margin_head numeric;
	v_margin_dbb numeric;
	v_margin_tbb numeric;
	v_margin_dbsh numeric;
	v_margin_btb numeric;
	v_margin_ntb numeric;
	v_margin_tnb numeric;
	v_margin_dnb numeric;
	v_margin_total_kvml numeric;

	-- Hiệu suất trên/vốn (%)
	v_hst_von_head numeric;
	v_hst_von_dbb numeric;
	v_hst_von_tbb numeric;
	v_hst_von_dbsh numeric;
	v_hst_von_btb numeric;
	v_hst_von_ntb numeric;
	v_hst_von_tnb numeric;
	v_hst_von_dnb numeric;
	v_hst_von_total_kvml numeric;

	-- Hiệu suất BQ/ Nhân sự
	v_hsbqns_head numeric;
	v_hsbqns_dbb numeric;
	v_hsbqns_tbb numeric;
	v_hsbqns_dbsh numeric;
	v_hsbqns_btb numeric;
	v_hsbqns_ntb numeric;
	v_hsbqns_tnb numeric;
	v_hsbqns_dnb numeric;
	v_hsbqns_total_kvml numeric;

	-- report 2
	v_month_num INTEGER;
    v_ltn_column VARCHAR;
	v_psdn_column varchar;
	v_approved_rate_column varchar;

	v_city_list TEXT;
	v_area_cde VARCHAR;
	v_cir numeric;
	v_margin numeric;
	v_hst_von numeric;
	v_hsbqns numeric;



begin 
	-- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Nguyen Phan Huynh Thang 
    -- Ngày tạo: 2025-05-18

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: Nguyen Phan Huynh Thang 
    -- Ngày cập nhật: 2025-05-18
    -- Mục đích cập nhật: Mô tả mục đích sửa đổi, nâng cấp, hoặc sửa lỗi

    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: Xử lý tham số và khởi tạo biến
    -- Bước 2: Thực hiện các câu lệnh SQL và xử lý logic
    -- Bước 3: Gán giá trị cho biến nếu cần
    -- Bước 4: Cập nhật thông tin người cập nhật và thời gian cập nhật
    -- Bước 5: Trả về kết quả nếu cần
    -- Bước 6: Xử lý ngoại lệ và ghi log (nếu cần)

    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
    -- ---------------------
   -- Bước 1: Xử lý tham số và khởi tạo biến
	if p_rp_month is null then 
		p_rp_month := to_char(current_date, 'YYYYMM');
	end if;

	v_start_log_time := now();

	v_start_rp_month := 202301;
	-- 
	v_month_num := CAST(SUBSTRING(p_rp_month::VARCHAR FROM 5 FOR 2) AS INTEGER);

	v_ltn_column := CASE v_month_num
        WHEN 1 THEN 'ltn_jan'
        WHEN 2 THEN 'ltn_feb'
        WHEN 3 THEN 'ltn_mar'
        WHEN 4 THEN 'ltn_apr'
        WHEN 5 THEN 'ltn_may'
        WHEN 6 THEN 'ltn_jun'
        WHEN 7 THEN 'ltn_july'
        WHEN 8 THEN 'ltn_aug'
        WHEN 9 THEN 'ltn_sep'
        WHEN 10 THEN 'ltn_oct'
        WHEN 11 THEN 'ltn_nov'
        WHEN 12 THEN 'ltn_dec'
    END;

	v_psdn_column := CASE v_month_num
		WHEN 1 THEN 'psdn_jan'
		WHEN 2 THEN 'psdn_feb'
		WHEN 3 THEN 'psdn_mar'
		WHEN 4 THEN 'psdn_apr'
		WHEN 5 THEN 'psdn_may'
		WHEN 6 THEN 'psdn_jun'
		WHEN 7 THEN 'psdn_july'
		WHEN 8 THEN 'psdn_aug'
		WHEN 9 THEN 'psdn_sep'
		WHEN 10 THEN 'psdn_oct'
		WHEN 11 THEN 'psdn_nov'
		WHEN 12 THEN 'psdn_dec'
	END;

	v_approved_rate_column := CASE v_month_num
		WHEN 1 THEN 'approved_rate_jan'
		WHEN 2 THEN 'approved_rate_feb'
		WHEN 3 THEN 'approved_rate_mar'
		WHEN 4 THEN 'approved_rate_apr'
		WHEN 5 THEN 'approved_rate_may'
		WHEN 6 THEN 'approved_rate_jun'
		WHEN 7 THEN 'approved_rate_july'
		WHEN 8 THEN 'approved_rate_aug'
		WHEN 9 THEN 'approved_rate_sep'
		WHEN 10 THEN 'approved_rate_oct'
		WHEN 11 THEN 'approved_rate_nov'
		WHEN 12 THEN 'approved_rate_dec'
	end;



	-- Bước 2: Thực hiện các câu lệnh SQL và xử lý logic
	-- ghi log thoi gian bat dau 
	INSERT INTO public.log_tracking
	(procedure_name, start_time, is_successful, rec_created_dt)
	VALUES('fact_report_monthly_prc', v_start_log_time,  false,  now())
	returning id into v_log_id;

	-- xoá dữ liệu trong bảng tại ngày cần chạy lại đi và đổ dữ liệu mới vào
	delete from fact_backdate_funding_monthly
	where month_key = p_rp_month  or month_key is null;

	delete from fact_backdate_asm_monthly
	where month_key = p_rp_month  or month_key is null;

	-- insert data moi vao fact_backdate_funding_monthly
	
	-- Lãi trong hạn
	-- Tổng cần phân bổ xuống cho ĐVML
	-- head 
	SELECT SUM(amount)  into v_laitronghan_head
	FROM fact_txn_month f
	WHERE account_code IN ( 702000030002, 702000030001,702000030102)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- kv dong bac bo 
	SELECT SUM(amount) into v_dbb_dvml_laitronghan
	FROM fact_txn_month f
	WHERE account_code IN ('702000030002', '702000030001', '702000030102')
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select duno_dbb::numeric / duno_tong into v_tile_pb_dbb_laitronghan
	from 
	(
		SELECT sum(outstanding_principal)  as duno_dbb
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND pos_city in ('Hà Giang', 'Tuyên Quang', 'Phú Thọ', 'Thái Nguyên', 'Bắc Kạn', 'Cao Bằng', 'Lạng Sơn'
		, 'Bắc Giang', 'Quảng Ninh')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as duno_tong
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;


	v_dbb_pb_laitronghan := v_laitronghan_head * v_tile_pb_dbb_laitronghan + v_dbb_dvml_laitronghan;

	-- Tây Bắc Bộ
	SELECT SUM(amount)  into v_tbb_dvml_laitronghan
	FROM fact_txn_month f
	WHERE account_code IN ('702000030002', '702000030001', '702000030102')
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select duno_dbb::numeric / duno_tong into v_tile_pb_tbb_laitronghan
	from 
	(
		SELECT sum(outstanding_principal)  as duno_dbb
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND pos_city in ('Lào Cai', 'Yên Bái', 'Điện Biên', 'Sơn La', 'Hòa Bình')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as duno_tong
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tbb_pb_laitronghan := v_laitronghan_head * v_tile_pb_tbb_laitronghan + v_tbb_dvml_laitronghan;
	
	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_dvml_laitronghan
	FROM fact_txn_month f
	WHERE account_code IN ('702000030002', '702000030001', '702000030102')
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select duno_dbb::numeric / duno_tong into v_tile_pb_dbsh_laitronghan
	from 
	(
		SELECT sum(outstanding_principal)  as duno_dbb
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND pos_city in ('Hà Nội', 'Hải Phòng', 'Vĩnh Phúc', 'Bắc Ninh', 'Hưng Yên', 'Hải Dương', 'Thái Bình', 'Nam Định', 'Ninh Bình', 'Hà Nam')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as duno_tong
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dbsh_pb_laitronghan := v_laitronghan_head * v_tile_pb_dbsh_laitronghan + v_dbsh_dvml_laitronghan;

	-- Bắc Trung Bộ
	SELECT SUM(amount)  into v_btb_dvml_laitronghan
	FROM fact_txn_month f
	WHERE account_code IN ('702000030002', '702000030001', '702000030102')
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select duno_dbb::numeric / duno_tong into v_tile_pb_btb_laitronghan
	from 
	(
		SELECT sum(outstanding_principal)  as duno_dbb
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND pos_city in ('Thanh Hoá', 'Nghệ An', 'Hà Tĩnh', 'Quảng Bình', 'Quảng Trị', 'Huế')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as duno_tong
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_btb_pb_laitronghan := v_laitronghan_head * v_tile_pb_btb_laitronghan + v_btb_dvml_laitronghan;

	-- Nam Trung Bộ
	SELECT SUM(amount)  into v_ntb_dvml_laitronghan
	FROM fact_txn_month f
	WHERE account_code IN ('702000030002', '702000030001', '702000030102')
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select duno_dbb::numeric / duno_tong into v_tile_pb_ntb_laitronghan
	from 
	(
		SELECT sum(outstanding_principal)  as duno_dbb
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND pos_city in (
		'Đà Nẵng'
		,'Quảng Nam'
		,'Quảng Ngãi'
		,'Bình Định'
		,'Phú Yên'
		,'Khánh Hoà'
		,'Ninh Thuận'
		,'Bình Thuận'
		,'Kon Tum'
		,'Gia Lai'
		,'Đắk Lắk'
		,'Đắk Nông'
		,'Lâm Đồng'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as duno_tong
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_ntb_pb_laitronghan := v_laitronghan_head * v_tile_pb_ntb_laitronghan + v_ntb_dvml_laitronghan;
	
	-- Tây Nam Bộ
	SELECT SUM(amount)  into v_tnb_dvml_laitronghan
	FROM fact_txn_month f
	WHERE account_code IN ('702000030002', '702000030001', '702000030102')
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select duno_dbb::numeric / duno_tong into v_tile_pb_tnb_laitronghan
	from 
	(
		SELECT sum(outstanding_principal)  as duno_dbb
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND pos_city in (
		'Cần Thơ'
		, 'Long An'
		, 'Đồng Tháp'
		, 'Tiền Giang'
		, 'An Giang'
		, 'Bến Tre'
		, 'Vĩnh Long'
		, 'Trà Vinh'
		, 'Hậu Giang'
		, 'Kiên Giang'
		, 'Sóc Trăng'
		, 'Bạc Liêu'
		, 'Cà Mau'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as duno_tong
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tnb_pb_laitronghan := v_laitronghan_head * v_tile_pb_tnb_laitronghan + v_tnb_dvml_laitronghan;

	-- Đông Nam Bộ
	SELECT SUM(amount)  into v_dnb_dvml_laitronghan
	FROM fact_txn_month f
	WHERE account_code IN ('702000030002', '702000030001', '702000030102')
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select duno_dbb::numeric / duno_tong into v_tile_pb_dnb_laitronghan
	from 
	(
		SELECT sum(outstanding_principal)  as duno_dbb
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND pos_city in (
		'Hồ Chí Minh'
		, 'Bà Rịa - Vũng Tàu'
		, 'Bình Dương'
		, 'Bình Phước'
		, 'Đồng Nai'
		, 'Tây Ninh'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as duno_tong
		FROM fact_kpi_month
		WHERE max_bucket = 1 
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dnb_pb_laitronghan := v_laitronghan_head * v_tile_pb_dnb_laitronghan + v_dnb_dvml_laitronghan;
	
	-- total 
	v_kvml_pb_laitronghan_total := v_dbb_pb_laitronghan + v_tbb_pb_laitronghan + v_dbsh_pb_laitronghan + v_btb_pb_laitronghan + v_ntb_pb_laitronghan + v_tnb_pb_laitronghan +  v_dnb_pb_laitronghan;
	-- insert 
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(9, v_laitronghan_head, 0, 0, 0, v_laitronghan_head, v_dbb_pb_laitronghan, v_tbb_pb_laitronghan, v_dbsh_pb_laitronghan, v_btb_pb_laitronghan, v_ntb_pb_laitronghan, v_tnb_pb_laitronghan, v_dnb_pb_laitronghan, v_kvml_pb_laitronghan_total, p_rp_month);
	
	-------Lãi quá hạn---------------------------------
	-- head
	SELECT SUM(amount) into v_laiquahan_head
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;
	
	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_dvml_laiquahan
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select  sum_dvml::numeric / sum_total into v_tile_pb_dbb_laiquahan 
	from 
	(
		SELECT sum(outstanding_principal) as sum_dvml
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND pos_city in ('Hà Giang', 'Tuyên Quang', 'Phú Thọ', 'Thái Nguyên', 'Bắc Kạn', 'Cao Bằng', 'Lạng Sơn'
		, 'Bắc Giang', 'Quảng Ninh')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as sum_total
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dbb_pb_laiquahan := v_laiquahan_head * v_tile_pb_dbb_laiquahan + v_dbb_dvml_laiquahan;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_dvml_laiquahan
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select  sum_dvml::numeric / sum_total into v_tile_pb_tbb_laiquahan 
	from 
	(
		SELECT sum(outstanding_principal) as sum_dvml
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND pos_city in ('Lào Cai', 'Yên Bái', 'Điện Biên', 'Sơn La', 'Hòa Bình')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as sum_total
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tbb_pb_laiquahan := v_laiquahan_head * v_tile_pb_tbb_laiquahan + v_tbb_dvml_laiquahan;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_dvml_laiquahan
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select  sum_dvml::numeric / sum_total into v_tile_pb_dbsh_laiquahan 
	from 
	(
		SELECT sum(outstanding_principal) as sum_dvml
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND pos_city in ('Hà Nội', 'Hải Phòng', 'Vĩnh Phúc', 'Bắc Ninh', 'Hưng Yên', 'Hải Dương', 'Thái Bình', 'Nam Định', 'Ninh Bình', 'Hà Nam')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as sum_total
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dbsh_pb_laiquahan := v_laiquahan_head * v_tile_pb_dbsh_laiquahan + v_dbsh_dvml_laiquahan;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_dvml_laiquahan
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select  sum_dvml::numeric / sum_total into v_tile_pb_btb_laiquahan 
	from 
	(
		SELECT sum(outstanding_principal) as sum_dvml
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND pos_city in ('Thanh Hoá', 'Nghệ An', 'Hà Tĩnh', 'Quảng Bình', 'Quảng Trị', 'Huế')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as sum_total
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_btb_pb_laiquahan := v_laiquahan_head * v_tile_pb_btb_laiquahan + v_btb_dvml_laiquahan;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_dvml_laiquahan
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select  sum_dvml::numeric / sum_total into v_tile_pb_ntb_laiquahan 
	from 
	(
		SELECT sum(outstanding_principal) as sum_dvml
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND pos_city in (
		'Đà Nẵng'
		,'Quảng Nam'
		,'Quảng Ngãi'
		,'Bình Định'
		,'Phú Yên'
		,'Khánh Hoà'
		,'Ninh Thuận'
		,'Bình Thuận'
		,'Kon Tum'
		,'Gia Lai'
		,'Đắk Lắk'
		,'Đắk Nông'
		,'Lâm Đồng'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as sum_total
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_ntb_pb_laiquahan := v_laiquahan_head * v_tile_pb_ntb_laiquahan + v_ntb_dvml_laiquahan;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_dvml_laiquahan
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select  sum_dvml::numeric / sum_total into v_tile_pb_tnb_laiquahan 
	from 
	(
		SELECT sum(outstanding_principal) as sum_dvml
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND pos_city in (
		'Cần Thơ'
		, 'Long An'
		, 'Đồng Tháp'
		, 'Tiền Giang'
		, 'An Giang'
		, 'Bến Tre'
		, 'Vĩnh Long'
		, 'Trà Vinh'
		, 'Hậu Giang'
		, 'Kiên Giang'
		, 'Sóc Trăng'
		, 'Bạc Liêu'
		, 'Cà Mau'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as sum_total
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tnb_pb_laiquahan := v_laiquahan_head * v_tile_pb_tnb_laiquahan + v_tnb_dvml_laiquahan;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_dvml_laiquahan
	FROM fact_txn_month f
	WHERE account_code IN (702000030012, 702000030112)
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select  sum_dvml::numeric / sum_total into v_tile_pb_dnb_laiquahan 
	from 
	(
		SELECT sum(outstanding_principal) as sum_dvml
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND pos_city in (
		'Hồ Chí Minh'
		, 'Bà Rịa - Vũng Tàu'
		, 'Bình Dương'
		, 'Bình Phước'
		, 'Đồng Nai'
		, 'Tây Ninh'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal) as sum_total
		FROM fact_kpi_month
		WHERE max_bucket = 2
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dnb_pb_laiquahan := v_laiquahan_head * v_tile_pb_dnb_laiquahan + v_dnb_dvml_laiquahan;
	
	-- total kvml
	v_kvml_pb_laiquahan_total := v_dbb_pb_laiquahan + v_tbb_pb_laiquahan + v_dbsh_pb_laiquahan + v_btb_pb_laiquahan + v_ntb_pb_laiquahan + v_tnb_pb_laiquahan +  v_dnb_pb_laiquahan;
	-- insert 
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(10,v_laiquahan_head, 0, 0, 0, v_laiquahan_head, v_dbb_pb_laiquahan, v_tbb_pb_laiquahan, v_dbsh_pb_laiquahan, v_btb_pb_laiquahan, v_ntb_pb_laiquahan, v_tnb_pb_laiquahan, v_dnb_pb_laiquahan, v_kvml_pb_laiquahan_total, p_rp_month);

	---------------------------------------------Phí Bảo hiểm----------------------------------------------------------
	-- head
	SELECT SUM(amount) into v_pbh_head
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_dvml_pbh
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sl_kv::numeric / sl_tong into v_tile_pb_dbb_pbh
	from 
	(
		SELECT count(f.psdn) as sl_kv
		FROM fact_kpi_month f
		WHERE psdn = 1
		and pos_city in ('Hà Giang', 'Tuyên Quang', 'Phú Thọ', 'Thái Nguyên', 'Bắc Kạn', 'Cao Bằng', 'Lạng Sơn'
		, 'Bắc Giang', 'Quảng Ninh')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT count(f.psdn) as  sl_tong
		FROM fact_kpi_month f
		WHERE psdn = 1
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dbb_pb_pbh := v_pbh_head * v_tile_pb_dbb_pbh + v_dbb_dvml_pbh;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_dvml_pbh
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sl_kv::numeric / sl_tong into v_tile_pb_tbb_pbh
	from 
	(
		SELECT count(f.psdn) as sl_kv
		FROM fact_kpi_month f
		WHERE psdn = 1
		and pos_city in ('Lào Cai', 'Yên Bái', 'Điện Biên', 'Sơn La', 'Hòa Bình')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT count(f.psdn) as  sl_tong
		FROM fact_kpi_month f
		WHERE psdn = 1
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tbb_pb_pbh := v_pbh_head * v_tile_pb_tbb_pbh + v_tbb_dvml_pbh;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_dvml_pbh
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sl_kv::numeric / sl_tong into v_tile_pb_dbsh_pbh
	from 
	(
		SELECT count(f.psdn) as sl_kv
		FROM fact_kpi_month f
		WHERE psdn = 1
		and pos_city in ('Hà Nội', 'Hải Phòng', 'Vĩnh Phúc', 'Bắc Ninh', 'Hưng Yên', 'Hải Dương', 'Thái Bình', 'Nam Định', 'Ninh Bình', 'Hà Nam')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT count(f.psdn) as  sl_tong
		FROM fact_kpi_month f
		WHERE psdn = 1
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dbsh_pb_pbh := v_pbh_head * v_tile_pb_dbsh_pbh + v_dbsh_dvml_pbh;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_dvml_pbh
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sl_kv::numeric / sl_tong into v_tile_pb_btb_pbh
	from 
	(
		SELECT count(f.psdn) as sl_kv
		FROM fact_kpi_month f
		WHERE psdn = 1
		and pos_city in ('Thanh Hoá', 'Nghệ An', 'Hà Tĩnh', 'Quảng Bình', 'Quảng Trị', 'Huế')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT count(f.psdn) as  sl_tong
		FROM fact_kpi_month f
		WHERE psdn = 1
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_btb_pb_pbh := v_pbh_head * v_tile_pb_btb_pbh + v_btb_dvml_pbh;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_dvml_pbh
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sl_kv::numeric / sl_tong into v_tile_pb_ntb_pbh
	from 
	(
		SELECT count(f.psdn) as sl_kv
		FROM fact_kpi_month f
		WHERE psdn = 1
		and pos_city in (
		'Đà Nẵng'
		,'Quảng Nam'
		,'Quảng Ngãi'
		,'Bình Định'
		,'Phú Yên'
		,'Khánh Hoà'
		,'Ninh Thuận'
		,'Bình Thuận'
		,'Kon Tum'
		,'Gia Lai'
		,'Đắk Lắk'
		,'Đắk Nông'
		,'Lâm Đồng'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT count(f.psdn) as  sl_tong
		FROM fact_kpi_month f
		WHERE psdn = 1
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_ntb_pb_pbh := v_pbh_head * v_tile_pb_ntb_pbh + v_ntb_dvml_pbh;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_dvml_pbh
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sl_kv::numeric / sl_tong into v_tile_pb_tnb_pbh
	from 
	(
		SELECT count(f.psdn) as sl_kv
		FROM fact_kpi_month f
		WHERE psdn = 1
		and pos_city in (
		'Cần Thơ'
		, 'Long An'
		, 'Đồng Tháp'
		, 'Tiền Giang'
		, 'An Giang'
		, 'Bến Tre'
		, 'Vĩnh Long'
		, 'Trà Vinh'
		, 'Hậu Giang'
		, 'Kiên Giang'
		, 'Sóc Trăng'
		, 'Bạc Liêu'
		, 'Cà Mau'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT count(f.psdn) as  sl_tong
		FROM fact_kpi_month f
		WHERE psdn = 1
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tnb_pb_pbh := v_pbh_head * v_tile_pb_tnb_pbh + v_tnb_dvml_pbh;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_dvml_pbh
	FROM fact_txn_month f
	WHERE account_code = 716000000001
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sl_kv::numeric / sl_tong into v_tile_pb_dnb_pbh
	from 
	(
		SELECT count(f.psdn) as sl_kv
		FROM fact_kpi_month f
		WHERE psdn = 1
		and pos_city in (
		'Hồ Chí Minh'
		, 'Bà Rịa - Vũng Tàu'
		, 'Bình Dương'
		, 'Bình Phước'
		, 'Đồng Nai'
		, 'Tây Ninh'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT count(f.psdn) as  sl_tong
		FROM fact_kpi_month f
		WHERE psdn = 1
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dnb_pb_pbh := v_pbh_head * v_tile_pb_dnb_pbh + v_dnb_dvml_pbh;
	-- total kvml
	v_kvml_pb_pbh_total := v_dbb_pb_pbh + v_tbb_pb_pbh + v_dbsh_pb_pbh + v_btb_pb_pbh + v_ntb_pb_pbh + v_tnb_pb_pbh +  v_dnb_pb_pbh;
	-- insert 
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(11, v_pbh_head, 0, 0, 0, v_pbh_head, v_dbb_pb_pbh, v_tbb_pb_pbh, v_dbsh_pb_pbh, v_btb_pb_pbh, v_ntb_pb_pbh, v_tnb_pb_pbh, v_dnb_pb_pbh, v_kvml_pb_pbh_total, p_rp_month);

	---------------------------------------------Phí tăng hạn mức--------------------------------
	-- head
	SELECT SUM(amount)  into v_thm_head
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount)  into v_dbb_dvml_thm
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbb_pb_thm := v_thm_head * v_tile_pb_dbb_laitronghan + v_dbb_dvml_thm;

	-- Tây Bắc Bộ
	SELECT SUM(amount)  into v_tbb_dvml_thm
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tbb_pb_thm := v_thm_head * v_tile_pb_tbb_laitronghan + v_tbb_dvml_thm;

	-- ĐB Sông Hồng
	SELECT SUM(amount)  into v_dbsh_dvml_thm
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbsh_pb_thm := v_thm_head * v_tile_pb_dbsh_laitronghan + v_dbsh_dvml_thm;

	-- Bắc Trung Bộ
	SELECT SUM(amount)  into v_btb_dvml_thm
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_btb_pb_thm := v_thm_head * v_tile_pb_btb_laitronghan + v_btb_dvml_thm;

	-- Nam Trung Bộ
	SELECT SUM(amount)  into v_ntb_dvml_thm
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_ntb_pb_thm := v_thm_head * v_tile_pb_ntb_laitronghan + v_ntb_dvml_thm;

	-- Tây Nam Bộ
	SELECT SUM(amount)  into v_tnb_dvml_thm
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tnb_pb_thm := v_thm_head * v_tile_pb_tnb_laitronghan + v_tnb_dvml_thm;

	-- Đông Nam Bộ
	SELECT SUM(amount)  into v_dnb_dvml_thm
	FROM fact_txn_month f
	WHERE account_code = 719000030002
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dnb_pb_thm := v_thm_head * v_tile_pb_dnb_laitronghan + v_dnb_dvml_thm;

	-- total kvml
	v_kvml_pb_thm_total := v_dbb_pb_thm + v_tbb_pb_thm + v_dbsh_pb_thm + v_btb_pb_thm + v_ntb_pb_thm + v_tnb_pb_thm +  v_dnb_pb_thm;
	-- insert 
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(12, v_thm_head, 0, 0, 0, v_thm_head, v_dbb_pb_thm, v_tbb_pb_thm, v_dbsh_pb_thm, v_btb_pb_thm, v_ntb_pb_thm, v_tnb_pb_thm, v_dnb_pb_thm, v_kvml_pb_thm_total, p_rp_month);


	--------------------------- Phí thanh toán chậm, thu từ ngoại bảng, khác… ------------------------------
	-- head
	SELECT SUM(amount) into v_ttc_head
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_dvml_ttc
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;


	select tl_kv::numeric / tl_tong into v_tile_pb_dbb_ttc
	from 
	(
		SELECT sum(outstanding_principal)  as tl_kv
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		and pos_city in ('Hà Giang', 'Tuyên Quang', 'Phú Thọ', 'Thái Nguyên', 'Bắc Kạn', 'Cao Bằng', 'Lạng Sơn'
		, 'Bắc Giang', 'Quảng Ninh')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as tl_tong
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;
	v_dbb_pb_ttc := v_ttc_head * v_tile_pb_dbb_ttc + v_dbb_dvml_ttc;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_dvml_ttc
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;


	select tl_kv::numeric / tl_tong into v_tile_pb_tbb_ttc
	from 
	(
		SELECT sum(outstanding_principal)  as tl_kv
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		and pos_city in ('Lào Cai', 'Yên Bái', 'Điện Biên', 'Sơn La', 'Hòa Bình')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as tl_tong
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;
	v_tbb_pb_ttc := v_ttc_head * v_tile_pb_tbb_ttc + v_tbb_dvml_ttc;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_dvml_ttc
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;


	select tl_kv::numeric / tl_tong into v_tile_pb_dbsh_ttc
	from 
	(
		SELECT sum(outstanding_principal)  as tl_kv
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		and pos_city in ('Hà Nội', 'Hải Phòng', 'Vĩnh Phúc', 'Bắc Ninh', 'Hưng Yên', 'Hải Dương', 'Thái Bình', 'Nam Định', 'Ninh Bình', 'Hà Nam')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as tl_tong
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;
	v_dbsh_pb_ttc := v_ttc_head * v_tile_pb_dbsh_ttc + v_dbsh_dvml_ttc;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_dvml_ttc
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;


	select tl_kv::numeric / tl_tong into v_tile_pb_btb_ttc
	from 
	(
		SELECT sum(outstanding_principal)  as tl_kv
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		and pos_city in ('Thanh Hoá', 'Nghệ An', 'Hà Tĩnh', 'Quảng Bình', 'Quảng Trị', 'Huế')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as tl_tong
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;
	v_btb_pb_ttc := v_ttc_head * v_tile_pb_btb_ttc + v_btb_dvml_ttc;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_dvml_ttc
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;


	select tl_kv::numeric / tl_tong into v_tile_pb_ntb_ttc
	from 
	(
		SELECT sum(outstanding_principal)  as tl_kv
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		and pos_city in (
		'Đà Nẵng'
		,'Quảng Nam'
		,'Quảng Ngãi'
		,'Bình Định'
		,'Phú Yên'
		,'Khánh Hoà'
		,'Ninh Thuận'
		,'Bình Thuận'
		,'Kon Tum'
		,'Gia Lai'
		,'Đắk Lắk'
		,'Đắk Nông'
		,'Lâm Đồng'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as tl_tong
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;
	v_ntb_pb_ttc := v_ttc_head * v_tile_pb_ntb_ttc + v_ntb_dvml_ttc;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_dvml_ttc
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;


	select tl_kv::numeric / tl_tong into v_tile_pb_tnb_ttc
	from 
	(
		SELECT sum(outstanding_principal)  as tl_kv
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		and  pos_city in (
		'Cần Thơ'
		, 'Long An'
		, 'Đồng Tháp'
		, 'Tiền Giang'
		, 'An Giang'
		, 'Bến Tre'
		, 'Vĩnh Long'
		, 'Trà Vinh'
		, 'Hậu Giang'
		, 'Kiên Giang'
		, 'Sóc Trăng'
		, 'Bạc Liêu'
		, 'Cà Mau'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as tl_tong
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;
	v_tnb_pb_ttc := v_ttc_head * v_tile_pb_tnb_ttc + v_tnb_dvml_ttc;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_dvml_ttc
	FROM fact_txn_month f
	WHERE account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;


	select tl_kv::numeric / tl_tong into v_tile_pb_dnb_ttc
	from 
	(
		SELECT sum(outstanding_principal)  as tl_kv
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		and  pos_city in (
		'Hồ Chí Minh'
		, 'Bà Rịa - Vũng Tàu'
		, 'Bình Dương'
		, 'Bình Phước'
		, 'Đồng Nai'
		, 'Tây Ninh'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x
	join 
	(
		SELECT sum(outstanding_principal)  as tl_tong
		FROM fact_kpi_month
		WHERE max_bucket between 2 and 5
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;
	v_dnb_pb_ttc := v_ttc_head * v_tile_pb_dnb_ttc + v_dnb_dvml_ttc;

	-- total kvml
	v_kvml_pb_ttc_total := v_dbb_pb_ttc + v_tbb_pb_ttc + v_dbsh_pb_ttc + v_btb_pb_ttc + v_ntb_pb_ttc + v_tnb_pb_ttc +  v_dnb_pb_ttc;

	-- insert
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(13, v_ttc_head, 0, 0, 0, v_ttc_head, v_dbb_pb_ttc, v_tbb_pb_ttc, v_dbsh_pb_ttc, v_btb_pb_ttc, v_ntb_pb_ttc, v_tnb_pb_ttc, v_dnb_pb_ttc, v_kvml_pb_ttc_total, p_rp_month);

	-------------------------------------------- Thu nhập từ hoạt động thẻ---------------------------------------
	-- head
	v_tnt_head := v_laitronghan_head + v_laiquahan_head + v_pbh_head + v_thm_head + v_ttc_head;
	v_tnt_dbb := v_dbb_dvml_laitronghan + v_dbb_dvml_laiquahan + v_dbb_dvml_pbh + v_dbb_dvml_thm + v_dbb_dvml_ttc;
	v_tnt_tbb := v_tbb_dvml_laitronghan + v_tbb_dvml_laiquahan + v_tbb_dvml_pbh + v_tbb_dvml_thm + v_tbb_dvml_ttc;
	v_tnt_dbsh := v_dbsh_dvml_laitronghan + v_dbsh_dvml_laiquahan + v_dbsh_dvml_pbh + v_dbsh_dvml_thm + v_dbsh_dvml_ttc;
	v_tnt_btb := v_btb_dvml_laitronghan + v_btb_dvml_laiquahan + v_btb_dvml_pbh + v_btb_dvml_thm + v_btb_dvml_ttc;
	v_tnt_ntb := v_ntb_dvml_laitronghan + v_ntb_dvml_laiquahan + v_ntb_dvml_pbh + v_ntb_dvml_thm + v_ntb_dvml_ttc;
	v_tnt_tnb := v_tnb_dvml_laitronghan + v_tnb_dvml_laiquahan + v_tnb_dvml_pbh + v_tnb_dvml_thm + v_tnb_dvml_ttc;
	v_tnt_dnb := v_dnb_dvml_laitronghan + v_dnb_dvml_laiquahan + v_dnb_dvml_pbh + v_dnb_dvml_thm + v_dnb_dvml_ttc;
	v_tnt_total := v_tnt_dbb + v_tnt_tbb + v_tnt_dbsh + v_tnt_btb + v_tnt_ntb + v_tnt_tnb + v_tnt_dnb;

	-- insert
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	values (4, v_tnt_head, 0, 0, 0, v_tnt_head, v_tnt_dbb, v_tnt_tbb, v_tnt_dbsh, v_tnt_btb, v_tnt_ntb, v_tnt_tnb, v_tnt_dnb, v_tnt_total, p_rp_month);

	------------------------------------------------- DT Nguon Von------------------------------------------------
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(14, 0, 0 ,0 ,0 , 0, 0, 0 ,0 , 0, 0, 0, 0, 0, p_rp_month);


	----------------------------------------------CP vốn TT 2---------------------------------------------------------
	-- head
	SELECT SUM(amount) into v_cpvtt2_head
	FROM fact_txn_month f
	WHERE account_code in (801000000001, 802000000001)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	--  lãi thu từ thẻ vay toàn hàng 
	v_lai_tvth := v_kvml_pb_laitronghan_total + v_kvml_pb_laiquahan_total ;

	-- doanh thu nguồn vốn toàn hàng
	SELECT SUM(amount) into v_doanh_thu_nguon_von_toan_hang
	FROM fact_txn_month f
	WHERE account_code IN (
		'702000040001','702000040002','703000000001','703000000002','703000000003','703000000004', '721000000041','721000000037','721000000039','721000000013','721000000014','721000000036','723000000014', '723000000037','821000000014','821000000037','821000000039','821000000041','821000000013','821000000036',
				'823000000014','823000000037','741031000001','741031000002','841000000001','841000000005','841000000004',
				'701000000001','701000000002','701037000001','701037000002','701000000101')
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;
	-- Đông Bắc Bộ
	v_dbb_pb_tt2 := v_cpvtt2_head * v_tnt_dbb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Tây Bắc Bộ
	v_tbb_pb_tt2 := v_cpvtt2_head * v_tnt_tbb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- ĐB Sông Hồng
	v_dbsh_pb_tt2 := v_cpvtt2_head * v_tnt_dbsh / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Bắc Trung Bộ
	v_btb_pb_tt2 := v_cpvtt2_head * v_tnt_btb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Nam Trung Bộ
	v_ntb_pb_tt2 := v_cpvtt2_head * v_tnt_ntb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Tây Nam Bộ
	v_tnb_pb_tt2 := v_cpvtt2_head * v_tnt_tnb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Đông Nam Bộ
	v_dnb_pb_tt2 := v_cpvtt2_head * v_tnt_dnb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- tổng kvml
	v_kvml_pb_tt2_total := v_dbb_pb_tt2 + v_tbb_pb_tt2 + v_dbsh_pb_tt2 + v_btb_pb_tt2 + v_ntb_pb_tt2 + v_tnb_pb_tt2 +  v_dnb_pb_tt2;

	-- insert
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(15, v_cpvtt2_head, 0, 0, 0, v_cpvtt2_head, v_dbb_pb_tt2, v_tbb_pb_tt2, v_dbsh_pb_tt2, v_btb_pb_tt2, v_ntb_pb_tt2, v_tnb_pb_tt2, v_dnb_pb_tt2, v_kvml_pb_tt2_total, p_rp_month);

	------------------------------------------------- CP vốn TT 1---------------------------------------------------------
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(16, 0, 0 ,0 ,0 , 0, 0, 0 ,0 , 0, 0, 0, 0, 0, p_rp_month);

	-------------------------------------- CP vốn CCTG ---------------------------------------------------------
	-- head
	SELECT SUM(amount) into v_cpcctg_head
	FROM fact_txn_month f
	WHERE account_code = 803000000001
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	v_cpcctg_dbb := v_cpcctg_head * v_tnt_dbb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Tây Bắc Bộ
	v_cpcctg_tbb := v_cpcctg_head * v_tnt_tbb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- ĐB Sông Hồng
	v_cpcctg_dbsh := v_cpcctg_head * v_tnt_dbsh / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Bắc Trung Bộ
	v_cpcctg_btb := v_cpcctg_head * v_tnt_btb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Nam Trung Bộ
	v_cpcctg_ntb := v_cpcctg_head * v_tnt_ntb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Tây Nam Bộ
	v_cpcctg_tnb := v_cpcctg_head * v_tnt_tnb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- Đông Nam Bộ
	v_cpcctg_dnb := v_cpcctg_head * v_tnt_dnb / (v_doanh_thu_nguon_von_toan_hang + v_lai_tvth) ;
	-- tổng kvml
	v_cpcctg_total := v_cpcctg_dbb + v_cpcctg_tbb + v_cpcctg_dbsh + v_cpcctg_btb + v_cpcctg_ntb + v_cpcctg_tnb +  v_cpcctg_dnb;

	-- insert
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(17, v_cpcctg_head, 0, 0, 0, v_cpcctg_head, v_cpcctg_dbb, v_cpcctg_tbb, v_cpcctg_dbsh, v_cpcctg_btb, v_cpcctg_ntb, v_cpcctg_tnb, v_cpcctg_dnb, v_cpcctg_total, p_rp_month);

	------------------------------------------------Chi phí thuần KDV------------------------------------------------
	-- head
	v_cp_kdv_head := v_cpvtt2_head + v_cpcctg_head;
	v_cp_kdv_dbb := v_dbb_pb_tt2 + v_cpcctg_dbb;
	v_cp_kdv_tbb := v_tbb_pb_tt2 + v_cpcctg_tbb;
	v_cp_kdv_dbsh := v_dbsh_pb_tt2 + v_cpcctg_dbsh;
	v_cp_kdv_btb := v_btb_pb_tt2 + v_cpcctg_btb;
	v_cp_kdv_ntb := v_ntb_pb_tt2 + v_cpcctg_ntb;
	v_cp_kdv_tnb := v_tnb_pb_tt2 + v_cpcctg_tnb;
	v_cp_kdv_dnb := v_dnb_pb_tt2 + v_cpcctg_dnb;
	v_cp_kdv_total := v_kvml_pb_tt2_total + v_cpcctg_total;

	-- insert
	INSERT INTO public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(5, v_cp_kdv_head, 0, 0, 0, v_cp_kdv_head, v_cp_kdv_dbb, v_cp_kdv_tbb, v_cp_kdv_dbsh, v_cp_kdv_btb, v_cp_kdv_ntb, v_cp_kdv_tnb, v_cp_kdv_dnb, v_cp_kdv_total, p_rp_month);

	---------------------------------------------------DT Fintech------------------------------------------------------------------
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(18, 0, 0 ,0 ,0 , 0, 0, 0 ,0 , 0, 0, 0, 0, 0, p_rp_month);

	-------------------------------------------------DT tiểu thương, cá nhân------------------------------------------------
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(19, 0, 0 ,0 ,0 , 0, 0, 0 ,0 , 0, 0, 0, 0, 0, p_rp_month);

	------------------------------------------------- DT Kinh doanh ------------------------------------------------
	-- head
	SELECT SUM(amount) into v_dt_kd_head
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
	,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount)  into v_dbb_kvml_dt_kd
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sum_kv::numeric / sum_total into v_tile_pb_dbb_dt_kd
	from 
	(
		SELECT sum(outstanding_principal)  as sum_kv
		FROM fact_kpi_month f
		WHERE pos_city in ('Hà Giang', 'Tuyên Quang', 'Phú Thọ', 'Thái Nguyên', 'Bắc Kạn', 'Cao Bằng', 'Lạng Sơn'
		, 'Bắc Giang', 'Quảng Ninh')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT sum(outstanding_principal)  as sum_total
		FROM fact_kpi_month
		WHERE kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dbb_pb_dt_kd := v_dt_kd_head * v_tile_pb_dbb_dt_kd + v_dbb_kvml_dt_kd;


	-- Tây Bắc Bộ
	SELECT SUM(amount)  into v_tbb_kvml_dt_kd	
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sum_kv::numeric / sum_total into v_tile_pb_tbb_dt_kd
	from 
	(
		SELECT sum(outstanding_principal)  as sum_kv
		FROM fact_kpi_month f
		WHERE pos_city in ('Lào Cai', 'Yên Bái', 'Điện Biên', 'Sơn La', 'Hòa Bình')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT sum(outstanding_principal)  as sum_total
		FROM fact_kpi_month
		WHERE kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tbb_pb_dt_kd := v_dt_kd_head * v_tile_pb_tbb_dt_kd + v_tbb_kvml_dt_kd;
	
	-- ĐB Sông Hồng
	SELECT SUM(amount)  into v_dbsh_kvml_dt_kd
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sum_kv::numeric / sum_total into v_tile_pb_dbsh_dt_kd
	from 
	(
		SELECT sum(outstanding_principal)  as sum_kv
		FROM fact_kpi_month f
		WHERE pos_city in ('Hà Nội', 'Hải Phòng', 'Vĩnh Phúc', 'Bắc Ninh', 'Hưng Yên', 'Hải Dương', 'Thái Bình', 'Nam Định', 'Ninh Bình', 'Hà Nam')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT sum(outstanding_principal)  as sum_total
		FROM fact_kpi_month
		WHERE kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dbsh_pb_dt_kd := v_dt_kd_head * v_tile_pb_dbsh_dt_kd + v_dbsh_kvml_dt_kd;

	-- Bắc Trung Bộ
	SELECT SUM(amount)  into v_btb_kvml_dt_kd
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sum_kv::numeric / sum_total into v_tile_pb_btb_dt_kd
	from 
	(
		SELECT sum(outstanding_principal)  as sum_kv
		FROM fact_kpi_month f
		WHERE pos_city in ('Thanh Hoá', 'Nghệ An', 'Hà Tĩnh', 'Quảng Bình', 'Quảng Trị', 'Huế')
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT sum(outstanding_principal)  as sum_total
		FROM fact_kpi_month
		WHERE kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_btb_pb_dt_kd := v_dt_kd_head * v_tile_pb_btb_dt_kd + v_btb_kvml_dt_kd;

	-- Nam Trung Bộ
	SELECT SUM(amount)  into v_ntb_kvml_dt_kd
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sum_kv::numeric / sum_total into v_tile_pb_ntb_dt_kd
	from 
	(
		SELECT sum(outstanding_principal)  as sum_kv
		FROM fact_kpi_month f
		WHERE pos_city in (
		'Đà Nẵng'
		,'Quảng Nam'
		,'Quảng Ngãi'
		,'Bình Định'
		,'Phú Yên'
		,'Khánh Hoà'
		,'Ninh Thuận'
		,'Bình Thuận'
		,'Kon Tum'
		,'Gia Lai'
		,'Đắk Lắk'
		,'Đắk Nông'
		,'Lâm Đồng'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT sum(outstanding_principal)  as sum_total
		FROM fact_kpi_month
		WHERE kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_ntb_pb_dt_kd := v_dt_kd_head * v_tile_pb_ntb_dt_kd + v_ntb_kvml_dt_kd;

	-- Tây Nam Bộ
	SELECT SUM(amount)  into v_tnb_kvml_dt_kd
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sum_kv::numeric / sum_total into v_tile_pb_tnb_dt_kd
	from 
	(
		SELECT sum(outstanding_principal)  as sum_kv
		FROM fact_kpi_month f
		WHERE pos_city in (
		'Cần Thơ'
		, 'Long An'
		, 'Đồng Tháp'
		, 'Tiền Giang'
		, 'An Giang'
		, 'Bến Tre'
		, 'Vĩnh Long'
		, 'Trà Vinh'
		, 'Hậu Giang'
		, 'Kiên Giang'
		, 'Sóc Trăng'
		, 'Bạc Liêu'
		, 'Cà Mau'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT sum(outstanding_principal)  as sum_total
		FROM fact_kpi_month
		WHERE kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_tnb_pb_dt_kd := v_dt_kd_head * v_tile_pb_tnb_dt_kd + v_tnb_kvml_dt_kd;

	-- Đông Nam Bộ
	SELECT SUM(amount)  into v_dnb_kvml_dt_kd
	FROM fact_txn_month f
	WHERE account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003
,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101)
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	select sum_kv::numeric / sum_total into v_tile_pb_dnb_dt_kd
	from 
	(
		SELECT sum(outstanding_principal)  as sum_kv
		FROM fact_kpi_month f
		WHERE pos_city in (
		'Hồ Chí Minh'
		, 'Bà Rịa - Vũng Tàu'
		, 'Bình Dương'
		, 'Bình Phước'
		, 'Đồng Nai'
		, 'Tây Ninh'
		)
		AND kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) x 
	join 
	(
		SELECT sum(outstanding_principal)  as sum_total
		FROM fact_kpi_month
		WHERE kpi_month BETWEEN v_start_rp_month  AND p_rp_month
	) y on 1 = 1;

	v_dnb_pb_dt_kd := v_dt_kd_head * v_tile_pb_dnb_dt_kd + v_dnb_kvml_dt_kd;

	-- tổng kvml
	v_kvml_pb_dt_kd_total := v_dbb_pb_dt_kd + v_tbb_pb_dt_kd + v_dbsh_pb_dt_kd + v_btb_pb_dt_kd + v_ntb_pb_dt_kd + v_tnb_pb_dt_kd +  v_dnb_pb_dt_kd;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(20, v_dt_kd_head, 0 ,0 ,0 , v_dt_kd_head, v_dbb_pb_dt_kd, v_tbb_pb_dt_kd ,v_dbsh_pb_dt_kd , v_btb_pb_dt_kd, v_ntb_pb_dt_kd, v_tnb_pb_dt_kd, v_dnb_pb_dt_kd, v_kvml_pb_dt_kd_total, p_rp_month);
	
	-----------------------------------------------------------CP hoa hồng -----------------------------------------
	-- head
	SELECT SUM(amount) into v_cp_hh_head 
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_kvml_cp_hh
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbb_pb_cp_hh := v_cp_hh_head * v_tile_pb_dbb_dt_kd + v_dbb_kvml_cp_hh;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_kvml_cp_hh
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tbb_pb_cp_hh := v_cp_hh_head * v_tile_pb_tbb_dt_kd + v_tbb_kvml_cp_hh;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_kvml_cp_hh
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbsh_pb_cp_hh := v_cp_hh_head * v_tile_pb_dbsh_dt_kd + v_dbsh_kvml_cp_hh;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_kvml_cp_hh
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_btb_pb_cp_hh := v_cp_hh_head * v_tile_pb_btb_dt_kd + v_btb_kvml_cp_hh;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_kvml_cp_hh
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_ntb_pb_cp_hh := v_cp_hh_head * v_tile_pb_ntb_dt_kd + v_ntb_kvml_cp_hh;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_kvml_cp_hh
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tnb_pb_cp_hh := v_cp_hh_head * v_tile_pb_tnb_dt_kd + v_tnb_kvml_cp_hh;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_kvml_cp_hh
	FROM fact_txn_month f
	WHERE account_code in (816000000001,816000000002,816000000003)
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dnb_pb_cp_hh := v_cp_hh_head * v_tile_pb_dnb_dt_kd + v_dnb_kvml_cp_hh;

	-- total
	v_kvml_pb_cp_hh_total := v_dbb_pb_cp_hh + v_tbb_pb_cp_hh + v_dbsh_pb_cp_hh + v_btb_pb_cp_hh + v_ntb_pb_cp_hh + v_tnb_pb_cp_hh + v_dnb_pb_cp_hh;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(21, v_cp_hh_head, 0 ,0 ,0 , v_cp_hh_head, v_dbb_pb_cp_hh, v_tbb_pb_cp_hh ,v_dbsh_pb_cp_hh , v_btb_pb_cp_hh, v_ntb_pb_cp_hh, v_tnb_pb_cp_hh, v_dnb_pb_cp_hh, v_kvml_pb_cp_hh_total, p_rp_month);

	------------------------------------------------- CP thuần KD khác  ------------------------------------------------
	-- head
	SELECT SUM(amount) into v_cp_kdk_head
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_kvml_cp_kdk
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbb_pb_cp_kdk := v_cp_kdk_head * v_tile_pb_dbb_dt_kd + v_dbb_kvml_cp_kdk;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_kvml_cp_kdk
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tbb_pb_cp_kdk := v_cp_kdk_head * v_tile_pb_tbb_dt_kd + v_tbb_kvml_cp_kdk;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_kvml_cp_kdk
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbsh_pb_cp_kdk := v_cp_kdk_head * v_tile_pb_dbsh_dt_kd + v_dbsh_kvml_cp_kdk;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_kvml_cp_kdk
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_btb_pb_cp_kdk := v_cp_kdk_head * v_tile_pb_btb_dt_kd + v_btb_kvml_cp_kdk;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_kvml_cp_kdk
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_ntb_pb_cp_kdk := v_cp_kdk_head * v_tile_pb_ntb_dt_kd + v_ntb_kvml_cp_kdk;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_kvml_cp_kdk
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tnb_pb_cp_kdk := v_cp_kdk_head * v_tile_pb_tnb_dt_kd + v_tnb_kvml_cp_kdk;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_kvml_cp_kdk
	FROM fact_txn_month f
	WHERE account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001
	,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101
	,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001)
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dnb_pb_cp_kdk := v_cp_kdk_head * v_tile_pb_dnb_dt_kd + v_dnb_kvml_cp_kdk;

	-- tổng kvml
	v_kvml_pb_cp_kdk_total := v_dbb_pb_cp_kdk + v_tbb_pb_cp_kdk + v_dbsh_pb_cp_kdk + v_btb_pb_cp_kdk + v_ntb_pb_cp_kdk + v_tnb_pb_cp_kdk + v_dnb_pb_cp_kdk;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(22, v_cp_kdk_head, 0 ,0 ,0 , v_cp_kdk_head, v_dbb_pb_cp_kdk, v_tbb_pb_cp_kdk ,v_dbsh_pb_cp_kdk , v_btb_pb_cp_kdk, v_ntb_pb_cp_kdk, v_tnb_pb_cp_kdk, v_dnb_pb_cp_kdk, v_kvml_pb_cp_kdk_total, p_rp_month);
	
	----------------------------------------------CP hợp tác kd tàu (net)---------------------------------------------
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(23, 0, 0 ,0 ,0 , 0, 0, 0 ,0 , 0, 0, 0, 0, 0, p_rp_month);

	-----------------------------------------------Chi phí thuần hoạt động khác---------------------------------------------
	v_cp_hdk_head := v_dt_kd_head + v_cp_hh_head  + v_cp_kdk_head;
	v_cp_hdk_dbb := v_dbb_pb_dt_kd + v_dbb_pb_cp_hh + v_dbb_pb_cp_kdk;
	v_cp_hdk_tbb := v_tbb_pb_dt_kd + v_tbb_pb_cp_hh + v_tbb_pb_cp_kdk;
	v_cp_hdk_dbsh := v_dbsh_pb_dt_kd + v_dbsh_pb_cp_hh + v_dbsh_pb_cp_kdk;
	v_cp_hdk_btb := v_btb_pb_dt_kd + v_btb_pb_cp_hh + v_btb_pb_cp_kdk;
	v_cp_hdk_ntb := v_ntb_pb_dt_kd + v_ntb_pb_cp_hh + v_ntb_pb_cp_kdk;
	v_cp_hdk_tnb := v_tnb_pb_dt_kd + v_tnb_pb_cp_hh + v_tnb_pb_cp_kdk;
	v_cp_hdk_dnb := v_dnb_pb_dt_kd + v_dnb_pb_cp_hh + v_dnb_pb_cp_kdk;
	v_cp_hdk_total := v_kvml_pb_dt_kd_total + v_kvml_pb_cp_hh_total + v_kvml_pb_cp_kdk_total;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(6, v_cp_hdk_head, 0 ,0 ,0 , v_cp_hdk_head, v_cp_hdk_dbb, v_cp_hdk_tbb ,v_cp_hdk_dbsh , v_cp_hdk_btb, v_cp_hdk_ntb, v_cp_hdk_tnb, v_cp_hdk_dnb, v_cp_hdk_total, p_rp_month);

	-------------------------------------------Tổng thu nhập hoạt động------------------------------------
	v_tong_tnhd_head := v_tnt_head + v_cp_kdv_head + v_cp_hdk_head;
	v_tong_tnhd_dbb := v_tnt_dbb + v_cp_kdv_dbb + v_cp_hdk_dbb;
	v_tong_tnhd_tbb := v_tnt_tbb + v_cp_kdv_tbb + v_cp_hdk_tbb;
	v_tong_tnhd_dbsh := v_tnt_dbsh + v_cp_kdv_dbsh + v_cp_hdk_dbsh;
	v_tong_tnhd_btb := v_tnt_btb + v_cp_kdv_btb + v_cp_hdk_btb;
	v_tong_tnhd_ntb := v_tnt_ntb + v_cp_kdv_ntb + v_cp_hdk_ntb;
	v_tong_tnhd_tnb := v_tnt_tnb + v_cp_kdv_tnb + v_cp_hdk_tnb;
	v_tong_tnhd_dnb := v_tnt_dnb + v_cp_kdv_dnb + v_cp_hdk_dnb;
	v_tong_tnhd_total := v_tnt_total + v_cp_kdv_total + v_cp_hdk_total;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(7, v_tong_tnhd_head, 0 ,0 ,0 , v_tong_tnhd_head, v_tong_tnhd_dbb, v_tong_tnhd_tbb ,v_tong_tnhd_dbsh , v_tong_tnhd_btb, v_tong_tnhd_ntb, v_tong_tnhd_tnb, v_tong_tnhd_dnb, v_tong_tnhd_total, p_rp_month);

	-------------------------------------------CP thuế, phí-------------------------------------------------
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(24, 0, 0 ,0 ,0 , 0, 0, 0 ,0 , 0, 0, 0, 0, 0, p_rp_month);

	-------------------------------------------2. Số lượng nhân sự ( Sale Manager )-----------------------------
	-- total
	select count(k.ltn_feb) into v_count_sm_total
	from kpi_asm_data k;

	-- Đông Bắc Bộ
	select count(k.ltn_feb) into v_count_sm_dbb
	from kpi_asm_data k
	where k.area_name = 'Đông Bắc Bộ';

	-- Tây Bắc Bộ
	select count(k.ltn_feb) into v_count_sm_tbb
	from kpi_asm_data k
	where k.area_name = 'Tây Bắc Bộ';

	-- ĐB Sông Hồng
	select count(k.ltn_feb) into v_count_sm_dbsh
	from kpi_asm_data k
	where k.area_name = 'Đồng Bằng Sông Hồng';

	-- Bắc Trung Bộ
	select count(k.ltn_feb) into v_count_sm_btb
	from kpi_asm_data k
	where k.area_name = 'Bắc Trung Bộ';

	-- Nam Trung Bộ
	select count(k.ltn_feb) into v_count_sm_ntb
	from kpi_asm_data k
	where k.area_name = 'Nam Trung Bộ';

	-- Tây Nam Bộ
	select count(k.ltn_feb) into v_count_sm_tnb
	from kpi_asm_data k
	where k.area_name = 'Tây Nam Bộ';

	-- Đông Nam Bộ
	select count(k.ltn_feb) into v_count_sm_dnb
	from kpi_asm_data k
	where k.area_name = 'Đông Nam Bộ';

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(2, 0, 0 ,0 ,0 , v_count_sm_total, v_count_sm_dbb, v_count_sm_tbb ,v_count_sm_dbsh , v_count_sm_btb, v_count_sm_ntb, v_count_sm_tnb, v_count_sm_dnb, v_count_sm_total, p_rp_month);



	-------------------------------------------CP nhân viên--------------------	---------------------------------
	-- head
	SELECT SUM(amount) into v_cp_nv_head
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ

	SELECT SUM(amount) into v_dbb_kvml_cp_nv
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tile_pb_dbb_cp_nv := v_count_sm_dbb / v_count_sm_total;

	v_dbb_pb_cp_nv := v_cp_nv_head * v_tile_pb_dbb_cp_nv + v_dbb_kvml_cp_nv;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_kvml_cp_nv
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tile_pb_tbb_cp_nv := v_count_sm_tbb / v_count_sm_total;

	v_tbb_pb_cp_nv := v_cp_nv_head * v_tile_pb_tbb_cp_nv + v_tbb_kvml_cp_nv;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_kvml_cp_nv
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tile_pb_dbsh_cp_nv := v_count_sm_dbsh / v_count_sm_total;

	v_dbsh_pb_cp_nv := v_cp_nv_head * v_tile_pb_dbsh_cp_nv + v_dbsh_kvml_cp_nv;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_kvml_cp_nv
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tile_pb_btb_cp_nv := v_count_sm_btb / v_count_sm_total;

	v_btb_pb_cp_nv := v_cp_nv_head * v_tile_pb_btb_cp_nv + v_btb_kvml_cp_nv;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_kvml_cp_nv
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tile_pb_ntb_cp_nv := v_count_sm_ntb / v_count_sm_total;

	v_ntb_pb_cp_nv := v_cp_nv_head * v_tile_pb_ntb_cp_nv + v_ntb_kvml_cp_nv;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_kvml_cp_nv
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tile_pb_tnb_cp_nv := v_count_sm_tnb / v_count_sm_total;

	v_tnb_pb_cp_nv := v_cp_nv_head * v_tile_pb_tnb_cp_nv + v_tnb_kvml_cp_nv;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_kvml_cp_nv
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '85%'
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tile_pb_dnb_cp_nv := v_count_sm_dnb / v_count_sm_total;

	v_dnb_pb_cp_nv := v_cp_nv_head * v_tile_pb_dnb_cp_nv + v_dnb_kvml_cp_nv;

	-- tổng kvml
	v_kvml_pb_cp_nv_total := v_dbb_pb_cp_nv + v_tbb_pb_cp_nv + v_dbsh_pb_cp_nv + v_btb_pb_cp_nv + v_ntb_pb_cp_nv + v_tnb_pb_cp_nv +  v_dnb_pb_cp_nv;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(25, v_cp_nv_head, 0 ,0 ,0 , v_cp_nv_head, v_dbb_pb_cp_nv, v_tbb_pb_cp_nv ,v_dbsh_pb_cp_nv , v_btb_pb_cp_nv, v_ntb_pb_cp_nv, v_tnb_pb_cp_nv, v_dnb_pb_cp_nv, v_kvml_pb_cp_nv_total, p_rp_month);


	----------------------------------------------CP quản lý-------------------------------------------------------------
	-- head
	SELECT SUM(amount) into v_cp_ql_head
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_kvml_cp_ql
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbb_pb_cp_ql := v_cp_ql_head * v_tile_pb_dbb_cp_nv + v_dbb_kvml_cp_ql;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_kvml_cp_ql
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tbb_pb_cp_ql := v_cp_ql_head * v_tile_pb_tbb_cp_nv + v_tbb_kvml_cp_ql;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_kvml_cp_ql
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbsh_pb_cp_ql := v_cp_ql_head * v_tile_pb_dbsh_cp_nv + v_dbsh_kvml_cp_ql;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_kvml_cp_ql
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_btb_pb_cp_ql := v_cp_ql_head * v_tile_pb_btb_cp_nv + v_btb_kvml_cp_ql;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_kvml_cp_ql
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_ntb_pb_cp_ql := v_cp_ql_head * v_tile_pb_ntb_cp_nv + v_ntb_kvml_cp_ql;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_kvml_cp_ql
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tnb_pb_cp_ql := v_cp_ql_head * v_tile_pb_tnb_cp_nv + v_tnb_kvml_cp_ql;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_kvml_cp_ql
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '86%'
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dnb_pb_cp_ql := v_cp_ql_head * v_tile_pb_dnb_cp_nv + v_dnb_kvml_cp_ql;

	-- tổng kvml
	v_kvml_pb_cp_ql_total := v_dbb_pb_cp_ql + v_tbb_pb_cp_ql + v_dbsh_pb_cp_ql + v_btb_pb_cp_ql + v_ntb_pb_cp_ql + v_tnb_pb_cp_ql +  v_dnb_pb_cp_ql;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(26, v_cp_ql_head, 0 ,0 ,0 , v_cp_ql_head, v_dbb_pb_cp_ql, v_tbb_pb_cp_ql ,v_dbsh_pb_cp_ql , v_btb_pb_cp_ql, v_ntb_pb_cp_ql, v_tnb_pb_cp_ql, v_dnb_pb_cp_ql, v_kvml_pb_cp_ql_total, p_rp_month);

	-------------------------------------------CP tài sản---------------------------------------------------
	-- head
	SELECT SUM(amount) into v_cp_ts_head
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_kvml_cp_ts
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbb_pb_cp_ts := v_cp_ts_head * v_tile_pb_dbb_cp_nv + v_dbb_kvml_cp_ts;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_kvml_cp_ts
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tbb_pb_cp_ts := v_cp_ts_head * v_tile_pb_tbb_cp_nv + v_tbb_kvml_cp_ts;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_kvml_cp_ts
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbsh_pb_cp_ts := v_cp_ts_head * v_tile_pb_dbsh_cp_nv + v_dbsh_kvml_cp_ts;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_kvml_cp_ts
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_btb_pb_cp_ts := v_cp_ts_head * v_tile_pb_btb_cp_nv + v_btb_kvml_cp_ts;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_kvml_cp_ts
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_ntb_pb_cp_ts := v_cp_ts_head * v_tile_pb_ntb_cp_nv + v_ntb_kvml_cp_ts;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_kvml_cp_ts
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tnb_pb_cp_ts := v_cp_ts_head * v_tile_pb_tnb_cp_nv + v_tnb_kvml_cp_ts;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_kvml_cp_ts
	FROM fact_txn_month f
	WHERE cast(account_code as varchar) like '87%'
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dnb_pb_cp_ts := v_cp_ts_head * v_tile_pb_dnb_cp_nv + v_dnb_kvml_cp_ts;

	-- tổng kvml
	v_kvml_pb_cp_ts_total := v_dbb_pb_cp_ts + v_tbb_pb_cp_ts + v_dbsh_pb_cp_ts + v_btb_pb_cp_ts + v_ntb_pb_cp_ts + v_tnb_pb_cp_ts +  v_dnb_pb_cp_ts;
	
	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(27, v_cp_ts_head, 0 ,0 ,0 , v_cp_ts_head, v_dbb_pb_cp_ts, v_tbb_pb_cp_ts ,v_dbsh_pb_cp_ts , v_btb_pb_cp_ts, v_ntb_pb_cp_ts, v_tnb_pb_cp_ts, v_dnb_pb_cp_ts, v_kvml_pb_cp_ts_total, p_rp_month);

	-------------------------------------------Tổng chi phí hoạt động------------------------------------
	v_tong_cp_hd_head := v_cp_nv_head + v_cp_ql_head + v_cp_ts_head;
	v_tong_cp_hd_dbb := v_dbb_pb_cp_nv + v_dbb_pb_cp_ql + v_dbb_pb_cp_ts;
	v_tong_cp_hd_tbb := v_tbb_pb_cp_nv + v_tbb_pb_cp_ql + v_tbb_pb_cp_ts;
	v_tong_cp_hd_dbsh := v_dbsh_pb_cp_nv + v_dbsh_pb_cp_ql + v_dbsh_pb_cp_ts;
	v_tong_cp_hd_btb := v_btb_pb_cp_nv + v_btb_pb_cp_ql + v_btb_pb_cp_ts;
	v_tong_cp_hd_ntb := v_ntb_pb_cp_nv + v_ntb_pb_cp_ql + v_ntb_pb_cp_ts;
	v_tong_cp_hd_tnb := v_tnb_pb_cp_nv + v_tnb_pb_cp_ql + v_tnb_pb_cp_ts;
	v_tong_cp_hd_dnb := v_dnb_pb_cp_nv + v_dnb_pb_cp_ql + v_dnb_pb_cp_ts;
	v_tong_cp_hd_total_kvml := v_kvml_pb_cp_nv_total + v_kvml_pb_cp_ql_total + v_kvml_pb_cp_ts_total;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(8, v_tong_cp_hd_head, 0 ,0 ,0 , v_tong_cp_hd_head, v_tong_cp_hd_dbb, v_tong_cp_hd_tbb ,v_tong_cp_hd_dbsh , v_tong_cp_hd_btb, v_tong_cp_hd_ntb, v_tong_cp_hd_tnb, v_tong_cp_hd_dnb, v_tong_cp_hd_total_kvml, p_rp_month);

	-------------------------------------------------Chi phí dự phòng------------------------------------------------
	-- head
	SELECT SUM(amount) into v_cp_dp_head
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'HEAD%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	-- Đông Bắc Bộ
	SELECT SUM(amount) into v_dbb_kvml_cp_dp
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'DVML.%.B.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbb_pb_cp_dp := v_cp_dp_head * v_tile_pb_dbb_ttc + v_dbb_kvml_cp_dp;

	-- Tây Bắc Bộ
	SELECT SUM(amount) into v_tbb_kvml_cp_dp
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'DVML.%.C.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tbb_pb_cp_dp := v_cp_dp_head * v_tile_pb_tbb_ttc + v_tbb_kvml_cp_dp;

	-- ĐB Sông Hồng
	SELECT SUM(amount) into v_dbsh_kvml_cp_dp
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'DVML.%.D.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dbsh_pb_cp_dp := v_cp_dp_head * v_tile_pb_dbsh_ttc + v_dbsh_kvml_cp_dp;

	-- Bắc Trung Bộ
	SELECT SUM(amount) into v_btb_kvml_cp_dp
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'DVML.%.E.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_btb_pb_cp_dp := v_cp_dp_head * v_tile_pb_btb_ttc + v_btb_kvml_cp_dp;

	-- Nam Trung Bộ
	SELECT SUM(amount) into v_ntb_kvml_cp_dp
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'DVML.%.F.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_ntb_pb_cp_dp := v_cp_dp_head * v_tile_pb_ntb_ttc + v_ntb_kvml_cp_dp;

	-- Tây Nam Bộ
	SELECT SUM(amount) into v_tnb_kvml_cp_dp
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'DVML.%.G.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_tnb_pb_cp_dp := v_cp_dp_head * v_tile_pb_tnb_ttc + v_tnb_kvml_cp_dp;

	-- Đông Nam Bộ
	SELECT SUM(amount) into v_dnb_kvml_cp_dp
	FROM fact_txn_month f
	WHERE f.account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001
	, 882200050101, 882200020101, 882200060001,790000050101, 882200030101)
	AND analysis_code LIKE 'DVML.%.H.%.%'
	AND CAST(TO_CHAR(transaction_date, 'YYYYMM') AS INT8) BETWEEN v_start_rp_month AND p_rp_month;

	v_dnb_pb_cp_dp := v_cp_dp_head * v_tile_pb_dnb_ttc + v_dnb_kvml_cp_dp;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(28, v_cp_dp_head, 0 ,0 ,0 , v_cp_dp_head, v_dbb_pb_cp_dp, v_tbb_pb_cp_dp ,v_dbsh_pb_cp_dp , v_btb_pb_cp_dp, v_ntb_pb_cp_dp, v_tnb_pb_cp_dp, v_dnb_pb_cp_dp, v_dbb_pb_cp_dp + v_tbb_pb_cp_dp + v_dbsh_pb_cp_dp + v_btb_pb_cp_dp + v_ntb_pb_cp_dp + v_tnb_pb_cp_dp +  v_dnb_pb_cp_dp, p_rp_month);

	--------------------------------------------1. Lợi nhuận trước thuế---------------------------------------------
	-- head
	v_lntt_head := v_tong_tnhd_head + v_tong_cp_hd_head + v_cp_dp_head;
	-- Đông Bắc Bộ
	v_lntt_dbb := v_tong_tnhd_dbb + v_tong_cp_hd_dbb + v_dbb_pb_cp_dp;
	-- Tây Bắc Bộ
	v_lntt_tbb := v_tong_tnhd_tbb + v_tong_cp_hd_tbb + v_tbb_pb_cp_dp;
	-- ĐB Sông Hồng
	v_lntt_dbsh := v_tong_tnhd_dbsh + v_tong_cp_hd_dbsh + v_dbsh_pb_cp_dp;
	-- Bắc Trung Bộ
	v_lntt_btb := v_tong_tnhd_btb + v_tong_cp_hd_btb + v_btb_pb_cp_dp;
	-- Nam Trung Bộ
	v_lntt_ntb := v_tong_tnhd_ntb + v_tong_cp_hd_ntb + v_ntb_pb_cp_dp;
	-- Tây Nam Bộ
	v_lntt_tnb := v_tong_tnhd_tnb + v_tong_cp_hd_tnb + v_tnb_pb_cp_dp;
	-- Đông Nam Bộ
	v_lntt_dnb := v_tong_tnhd_dnb + v_tong_cp_hd_dnb + v_dnb_pb_cp_dp;
	-- tổng kvml
	v_lntt_total_kvml := v_lntt_dbb + v_lntt_tbb + v_lntt_dbsh + v_lntt_btb + v_lntt_ntb + v_lntt_tnb +  v_lntt_dnb;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(1, v_lntt_head, 0 ,0 ,0 , v_lntt_head, v_lntt_dbb, v_lntt_tbb ,v_lntt_dbsh , v_lntt_btb, v_lntt_ntb, v_lntt_tnb, v_lntt_dnb, v_lntt_total_kvml, p_rp_month);

	-------------------------------------------- 3. Chỉ số tài chính-----------------------------------------
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(3, 0, 0 ,0 ,0 , 0, 0, 0 ,0 , 0, 0, 0, 0, 0, p_rp_month);

	--------------------------------------------- CIR (%) ------------------------------------------------
	v_cir_head := COALESCE((v_tong_cp_hd_head / NULLIF(v_tong_tnhd_head, 0)) * 100 * (-1), 0);
	v_cir_dbb := COALESCE((v_tong_cp_hd_dbb / NULLIF(v_tong_tnhd_dbb, 0)) * 100 * (-1), 0);
	v_cir_tbb := COALESCE((v_tong_cp_hd_tbb / NULLIF(v_tong_tnhd_tbb, 0)) * 100 * (-1), 0);
	v_cir_dbsh := COALESCE((v_tong_cp_hd_dbsh / NULLIF(v_tong_tnhd_dbsh, 0)) * 100 * (-1), 0);
	v_cir_btb := COALESCE((v_tong_cp_hd_btb / NULLIF(v_tong_tnhd_btb, 0)) * 100 * (-1), 0);
	v_cir_ntb := COALESCE((v_tong_cp_hd_ntb / NULLIF(v_tong_tnhd_ntb, 0)) * 100 * (-1), 0);
	v_cir_tnb := COALESCE((v_tong_cp_hd_tnb / NULLIF(v_tong_tnhd_tnb, 0)) * 100 * (-1), 0);
	v_cir_dnb := COALESCE((v_tong_cp_hd_dnb / NULLIF(v_tong_tnhd_dnb, 0)) * 100 * (-1), 0);
	v_cir_total_kvml := v_cir_dbb + v_cir_tbb + v_cir_dbsh + v_cir_btb + v_cir_ntb + v_cir_tnb +  v_cir_dnb;
	
	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(29, 0, 0 ,0 ,0 , v_cir_head, v_cir_dbb, v_cir_tbb ,v_cir_dbsh , v_cir_btb, v_cir_ntb, v_cir_tnb, v_cir_dnb, v_cir_total_kvml, p_rp_month);

	-----------------------------------------Margin (%)-----------------------------------------------
	v_margin_head := COALESCE((v_lntt_head / NULLIF(v_tnt_head + v_dt_kd_head , 0)) * 100, 0);
	v_margin_dbb := COALESCE((v_lntt_dbb / NULLIF(v_tnt_dbb + v_dbb_pb_dt_kd , 0)) * 100, 0);
	v_margin_tbb := COALESCE((v_lntt_tbb / NULLIF(v_tnt_tbb + v_tbb_pb_dt_kd , 0)) * 100, 0);
	v_margin_dbsh := COALESCE((v_lntt_dbsh / NULLIF(v_tnt_dbsh + v_dbsh_pb_dt_kd , 0)) * 100, 0);
	v_margin_btb := COALESCE((v_lntt_btb / NULLIF(v_tnt_btb + v_btb_pb_dt_kd , 0)) * 100, 0);
	v_margin_ntb := COALESCE((v_lntt_ntb / NULLIF(v_tnt_ntb + v_ntb_pb_dt_kd , 0)) * 100, 0);
	v_margin_tnb := COALESCE((v_lntt_tnb / NULLIF(v_tnt_tnb + v_tnb_pb_dt_kd , 0)) * 100, 0);
	v_margin_dnb := COALESCE((v_lntt_dnb / NULLIF(v_tnt_dnb + v_dnb_pb_dt_kd , 0)) * 100, 0);
	v_margin_total_kvml := v_margin_dbb + v_margin_tbb + v_margin_dbsh + v_margin_btb + v_margin_ntb + v_margin_tnb +  v_margin_dnb;

	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(30, 0, 0 ,0 ,0 , v_margin_head, v_margin_dbb, v_margin_tbb ,v_margin_dbsh , v_margin_btb, v_margin_ntb, v_margin_tnb, v_margin_dnb, v_margin_total_kvml, p_rp_month);

	-- Hiệu suất trên/vốn (%)
	v_hst_von_head := -COALESCE((v_lntt_head / NULLIF(v_cp_kdv_head , 0)) * 100, 0);
	v_hst_von_dbb := -COALESCE((v_lntt_dbb / NULLIF(v_cp_kdv_dbb , 0)) * 100, 0);
	v_hst_von_tbb := -COALESCE((v_lntt_tbb / NULLIF(v_cp_kdv_tbb , 0)) * 100, 0);
	v_hst_von_dbsh := -COALESCE((v_lntt_dbsh / NULLIF(v_cp_kdv_dbsh , 0)) * 100, 0);
	v_hst_von_btb := -COALESCE((v_lntt_btb / NULLIF(v_cp_kdv_btb , 0)) * 100, 0);
	v_hst_von_ntb := -COALESCE((v_lntt_ntb / NULLIF(v_cp_kdv_ntb , 0)) * 100, 0);
	v_hst_von_tnb := -COALESCE((v_lntt_tnb / NULLIF(v_cp_kdv_tnb , 0)) * 100, 0);
	v_hst_von_dnb := -COALESCE((v_lntt_dnb / NULLIF(v_cp_kdv_dnb , 0)) * 100, 0);
	v_hst_von_total_kvml := v_hst_von_dbb + v_hst_von_tbb + v_hst_von_dbsh + v_hst_von_btb + v_hst_von_ntb + v_hst_von_tnb +  v_hst_von_dnb;
	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(31, 0, 0 ,0 ,0 , v_hst_von_head, v_hst_von_dbb, v_hst_von_tbb ,v_hst_von_dbsh , v_hst_von_btb, v_hst_von_ntb, v_hst_von_tnb, v_hst_von_dnb, v_hst_von_total_kvml, p_rp_month);

	------------------------------------------------Hiệu suất BQ/ Nhân sự---------------------------------
	v_hsbqns_head := COALESCE((v_lntt_head / NULLIF(v_count_sm_total, 0)) , 0);
	v_hsbqns_dbb := COALESCE((v_lntt_dbb / NULLIF(v_count_sm_dbb, 0)) , 0);
	v_hsbqns_tbb := COALESCE((v_lntt_tbb / NULLIF(v_count_sm_tbb, 0)) , 0);
	v_hsbqns_dbsh := COALESCE((v_lntt_dbsh / NULLIF(v_count_sm_dbsh, 0)) , 0);
	v_hsbqns_btb := COALESCE((v_lntt_btb / NULLIF(v_count_sm_btb, 0)) , 0);
	v_hsbqns_ntb := COALESCE((v_lntt_ntb / NULLIF(v_count_sm_ntb, 0)) , 0);
	v_hsbqns_tnb := COALESCE((v_lntt_tnb / NULLIF(v_count_sm_tnb, 0)) , 0);
	v_hsbqns_dnb := COALESCE((v_lntt_dnb / NULLIF(v_count_sm_dnb, 0)) , 0);
	v_hsbqns_total_kvml := v_hsbqns_dbb + v_hsbqns_tbb + v_hsbqns_dbsh + v_hsbqns_btb + v_hsbqns_ntb + v_hsbqns_tnb +  v_hsbqns_dnb;
	-- insert
	insert into public.fact_backdate_funding_monthly
	(funding_id, tpb_head, tpb_mienbac, tpb_miennam, tpb_mientrung, tpv_total, kvml_dbb, kvml_tbb, kvml_dbsh, kvml_btb, kvml_ntb, kvml_tnb, kvml_dnb, kvml_total, month_key)
	VALUES(32, 0, 0 ,0 ,0 , v_hsbqns_head, v_hsbqns_dbb, v_hsbqns_tbb ,v_hsbqns_dbsh , v_hsbqns_btb, v_hsbqns_ntb, v_hsbqns_tnb, v_hsbqns_dnb, v_hsbqns_total_kvml, p_rp_month);

	------------------------------------------------report 2-----------------------------------------------
	-- insert month_key, area_cde, area_name, email
	EXECUTE format('
        INSERT INTO fact_backdate_asm_monthly (
            month_key,
            area_cde,
            area_name,
            email
        )
        SELECT
            CAST(%L AS int8) AS month_key,
            z.area_cde,
            z.area_name,
            z.email
        FROM 
		(
            SELECT
                x.area_cde,
                k.area_name,
                k.email
            FROM (
                SELECT a.area_cde, a.area_name
                FROM area_mapping a
            ) x
            JOIN kpi_asm_data k ON x.area_name = k.area_name
            WHERE k.%I IS NOT NULL
        ) z;
    ', p_rp_month, v_ltn_column );

	-- update ltn_avg
	EXECUTE format('
		UPDATE fact_backdate_asm_monthly f
		SET ltn_avg = k.ltn_avg
		from 
		(
			SELECT 
				k.email,
				(
					%s
				)::numeric / %s as ltn_avg
			FROM kpi_asm_data k
			WHERE k.%I is not null
		) k
		WHERE f.email = k.email
		and f.month_key = %L;
		', 
		CASE v_month_num
				WHEN 1 THEN 'COALESCE(k.ltn_jan, 0)'
				WHEN 2 THEN 'COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0)'
				WHEN 3 THEN 'COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0) + COALESCE(k.ltn_mar, 0)'
				WHEN 4 THEN 'COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0) + COALESCE(k.ltn_mar, 0) + COALESCE(k.ltn_apr, 0)'
				WHEN 5 THEN 'COALESCE(k.ltn_jan, 0) + COALESCE(k.ltn_feb, 0) + COALESCE(k.ltn_mar, 0) + COALESCE(k.ltn_apr, 0) + COALESCE(k.ltn_may, 0)'
		END,
		v_month_num::text,
		v_ltn_column,
		p_rp_month
		);

	-- rank_ltn_avg
	update fact_backdate_asm_monthly f
	set rank_ltn_avg = k.rank_ltn_avg
	from 
	(
		select email, rank() over(order by ltn_avg desc) as rank_ltn_avg
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- psdn_avg
	EXECUTE format('
		UPDATE fact_backdate_asm_monthly f
		SET psdn_avg = k.psdn_avg
		from 
		(
			SELECT 
				k.email,
				(
					%s
				)::numeric / %s as psdn_avg
			FROM kpi_asm_data k
			WHERE k.%I is not null
		) k
		WHERE f.email = k.email
		and f.month_key = %L;
		', 
		CASE v_month_num
				WHEN 1 THEN 'COALESCE(k.psdn_jan, 0)'
				WHEN 2 THEN 'COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0)'
				WHEN 3 THEN 'COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0) + COALESCE(k.psdn_mar, 0)'
				WHEN 4 THEN 'COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0) + COALESCE(k.psdn_mar, 0) + COALESCE(k.psdn_apr, 0)'
				WHEN 5 THEN 'COALESCE(k.psdn_jan, 0) + COALESCE(k.psdn_feb, 0) + COALESCE(k.psdn_mar, 0) + COALESCE(k.psdn_apr, 0) + COALESCE(k.psdn_may, 0)'
		end,
		v_month_num::text,
		v_psdn_column,
		p_rp_month
	);

	-- rank_psdn_avg
	update fact_backdate_asm_monthly f
	set rank_psdn_avg = k.rank_psdn_avg
	from 
	(
		select email, rank() over(order by psdn_avg desc) as rank_psdn_avg
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- approval_rate_avg
	EXECUTE format('
		UPDATE fact_backdate_asm_monthly f
		SET approval_rate_avg = k.approval_rate_avg
		from 
		(
			SELECT 
				k.email,
				(
					%s
				)::numeric / %s as approval_rate_avg
			FROM kpi_asm_data k
			WHERE k.%I is not null
		) k
		WHERE f.email = k.email
		and f.month_key = %L;
		', 
		CASE v_month_num
				WHEN 1 THEN 'COALESCE(k.approved_rate_jan, 0)'
				WHEN 2 THEN 'COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0)'
				WHEN 3 THEN 'COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0) + COALESCE(k.approved_rate_mar, 0)'
				WHEN 4 THEN 'COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0) + COALESCE(k.approved_rate_mar, 0) + COALESCE(k.approved_rate_apr, 0)'
				WHEN 5 THEN 'COALESCE(k.approved_rate_jan, 0) + COALESCE(k.approved_rate_feb, 0) + COALESCE(k.approved_rate_mar, 0) + COALESCE(k.approved_rate_apr, 0) + COALESCE(k.approved_rate_may, 0)'
		end,
		v_month_num::text,
		v_approved_rate_column,
		p_rp_month
	);

	--rank_approval_rate_avg
	update fact_backdate_asm_monthly f
	set rank_approval_rate_avg = k.rank_approval_rate_avg
	from 
	(
		select email, rank() over(order by approval_rate_avg desc) as rank_approval_rate_avg
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- npl_truoc_wo_luy_ke
	FOR v_area_cde, v_city_list IN
        SELECT area_cde, city_list
        FROM area_mapping
		order by area_cde
    LOOP
	
		EXECUTE format('
				WITH dunonhom345 AS (
					SELECT SUM(f.outstanding_principal) AS npl
					FROM fact_kpi_month f
					WHERE f.kpi_month = CAST(%L AS BIGINT)
					AND f.max_bucket IN (3, 4, 5)
					AND f.pos_city IN (%s)
				),
				writeoff_lk AS (
					SELECT SUM(f.write_off_balance_principal) AS wo_lk
					FROM fact_kpi_month f
					WHERE f.write_off_month BETWEEN %s AND CAST(%L AS BIGINT)
					AND f.pos_city IN (%s)
					
				),
				dunotong AS (
					SELECT SUM(f.outstanding_principal) AS duno_tong
					FROM fact_kpi_month f
					WHERE f.kpi_month = CAST(%L AS BIGINT)
					AND f.pos_city IN (%s)
				)
				UPDATE fact_backdate_asm_monthly
				SET npl_truoc_wo_luy_ke = (
					SELECT (npl + wo_lk)::NUMERIC / NULLIF(duno_tong + wo_lk, 0)
					FROM dunonhom345, writeoff_lk, dunotong
				)
				WHERE month_key = CAST(%L AS BIGINT)
				AND area_cde = %L;
			',
				p_rp_month,
				v_city_list,
				v_start_rp_month,
				p_rp_month,
				v_city_list,
				p_rp_month,
				v_city_list,
				p_rp_month,
				v_area_cde
			);
	END LOOP;

	-- rank_npl_truoc_wo_luy_ke
	UPDATE fact_backdate_asm_monthly f
	SET rank_npl_truoc_wo_luy_ke = k.rank_npl_truoc_wo_luy_ke
	FROM (
		SELECT email, rank() OVER (ORDER BY npl_truoc_wo_luy_ke asc) AS rank_npl_truoc_wo_luy_ke
		FROM fact_backdate_asm_monthly
		WHERE month_key = p_rp_month
	) k
	WHERE f.email = k.email
	and f.month_key = p_rp_month;
	
	-- Điểm Quy Mô
	UPDATE fact_backdate_asm_monthly
    SET diem_quy_mo = ranked.rank_ltn_avg + ranked.rank_psdn_avg + ranked.rank_approval_rate_avg + ranked.rank_npl_truoc_wo_luy_ke
    FROM (
        SELECT
            email,
			rank_ltn_avg,
			rank_psdn_avg,
			rank_approval_rate_avg,
			rank_npl_truoc_wo_luy_ke,
            month_key
        FROM fact_backdate_asm_monthly
        WHERE month_key = CAST(p_rp_month AS BIGINT)
    ) ranked
    WHERE fact_backdate_asm_monthly.email = ranked.email
        AND fact_backdate_asm_monthly.month_key = ranked.month_key;

	-- rank_ptkd
	update fact_backdate_asm_monthly f
	set rank_ptkd = k.rank_ptkd
	from 
	(
		select email, rank() over(order by diem_quy_mo asc) as rank_ptkd
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- cir
	FOR v_area_cde IN
        SELECT area_cde FROM area_mapping ORDER BY area_cde
    LOOP
        v_cir := CASE v_area_cde
            WHEN 'B' THEN v_cir_dbb
            WHEN 'C' THEN v_cir_tbb
            WHEN 'D' THEN v_cir_dbsh
            WHEN 'E' THEN v_cir_btb
            WHEN 'F' THEN v_cir_ntb
            WHEN 'G' THEN v_cir_tnb
            WHEN 'H' THEN v_cir_dnb
            ELSE NULL
        END;

        EXECUTE format('
            UPDATE fact_backdate_asm_monthly
            SET cir = %L
            WHERE month_key = CAST(%L AS BIGINT)
            AND area_cde = %L;
        ', v_cir, p_rp_month, v_area_cde);
    end loop;
	
	-- rank_cir
	update fact_backdate_asm_monthly f
	set rank_cir = k.rank_cir
	from 
	(
		select email, dense_rank() over(order by cir asc) as rank_cir
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- margin
	FOR v_area_cde IN
        SELECT area_cde FROM area_mapping ORDER BY area_cde
    LOOP
        v_margin := CASE v_area_cde
            WHEN 'B' THEN v_margin_dbb
            WHEN 'C' THEN v_margin_tbb
            WHEN 'D' THEN v_margin_dbsh
            WHEN 'E' THEN v_margin_btb
            WHEN 'F' THEN v_margin_ntb
            WHEN 'G' THEN v_margin_tnb
            WHEN 'H' THEN v_margin_dnb
            ELSE NULL
        END;

        EXECUTE format('
            UPDATE fact_backdate_asm_monthly
            SET margin = %L
            WHERE month_key = CAST(%L AS BIGINT)
            AND area_cde = %L;
        ', v_margin, p_rp_month, v_area_cde);
    end loop;

	-- rank_margin
	update fact_backdate_asm_monthly f
	set rank_margin = k.rank_margin
	from 
	(
		select email, dense_rank() over(order by margin desc) as rank_margin
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- Hiệu suất trên/vốn (%)
	FOR v_area_cde IN
		SELECT area_cde FROM area_mapping ORDER BY area_cde
	LOOP
		v_hst_von := CASE v_area_cde
			WHEN 'B' THEN v_hst_von_dbb
			WHEN 'C' THEN v_hst_von_tbb
			WHEN 'D' THEN v_hst_von_dbsh
			WHEN 'E' THEN v_hst_von_btb
			WHEN 'F' THEN v_hst_von_ntb
			WHEN 'G' THEN v_hst_von_tnb
			WHEN 'H' THEN v_hst_von_dnb
			ELSE NULL
		END;

		EXECUTE format('
			UPDATE fact_backdate_asm_monthly
			SET hs_von = %L
			WHERE month_key = CAST(%L AS BIGINT)
			AND area_cde = %L;
		', v_hst_von, p_rp_month, v_area_cde);
	end loop;

	-- rank_hs_von
	update fact_backdate_asm_monthly f
	set rank_hs_von = k.rank_hs_von
	from 
	(
		select email, dense_rank() over(order by hs_von desc) as rank_hs_von
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- hsbq_nhan_su
	FOR v_area_cde IN
		SELECT area_cde FROM area_mapping ORDER BY area_cde
	loop
		v_hsbqns := CASE v_area_cde
			WHEN 'B' THEN v_hsbqns_dbb
			WHEN 'C' THEN v_hsbqns_tbb
			WHEN 'D' THEN v_hsbqns_dbsh
			WHEN 'E' THEN v_hsbqns_btb
			WHEN 'F' THEN v_hsbqns_ntb
			WHEN 'G' THEN v_hsbqns_tnb
			WHEN 'H' THEN v_hsbqns_dnb
			ELSE NULL
		END;

		EXECUTE format('
			UPDATE fact_backdate_asm_monthly
			SET hsbq_nhan_su = %L
			WHERE month_key = CAST(%L AS BIGINT)
			AND area_cde = %L;
		', v_hsbqns, p_rp_month, v_area_cde);
	end loop;

	-- rank_hsbq_nhan_su
	update fact_backdate_asm_monthly f
	set rank_hsbq_nhan_su = k.rank_hsbq_nhan_su
	from 
	(
		select email, dense_rank() over(order by hsbq_nhan_su desc) as rank_hsbq_nhan_su
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- diem_fin
	UPDATE fact_backdate_asm_monthly
	SET diem_fin = ranked.diem_fin
	FROM (
		SELECT
			email,
			rank_cir + rank_margin + rank_hs_von + rank_hsbq_nhan_su AS diem_fin,
			month_key
		FROM fact_backdate_asm_monthly
		WHERE month_key = CAST(p_rp_month AS BIGINT)
	) ranked
	WHERE fact_backdate_asm_monthly.email = ranked.email
	AND fact_backdate_asm_monthly.month_key = ranked.month_key;

	-- rank_fin
	update fact_backdate_asm_monthly f
	set rank_fin = k.rank_fin
	from 
	(
		select email, rank() over(order by diem_fin asc) as rank_fin
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;

	-- tongdiem
	UPDATE fact_backdate_asm_monthly
    SET tongdiem = diem.diem_quy_mo + diem.diem_fin
    FROM (
        SELECT
            email,
			diem_quy_mo,
			diem_fin,
            month_key
        FROM fact_backdate_asm_monthly
        WHERE month_key = CAST(p_rp_month AS BIGINT)
    ) diem
    WHERE fact_backdate_asm_monthly.email = diem.email
        AND fact_backdate_asm_monthly.month_key = diem.month_key;

	-- rank_final
	update fact_backdate_asm_monthly f
	set rank_final = k.rank_final
	from 
	(
		select email, rank() over(order by tongdiem asc) as rank_final
		from fact_backdate_asm_monthly
		where month_key = p_rp_month
	) k
	where f.email = k.email
	and f.month_key = p_rp_month;




	---------------------------- -- Bước 6: Xử lý ngoại lệ và ghi log (nếu cần)------------------------
	-- Ghi log time ket thuc 
    v_end_log_time := clock_timestamp();
   
	update log_tracking 
	set end_time = v_end_log_time
		, is_successful = true 
	where id = v_log_id ;


	-- Ghi log loi 
	exception when others then 
		v_end_log_time := clock_timestamp();
		v_error_msg := sqlerrm || '-' || sqlstate;
		
	
		update log_tracking 
		set end_time = v_end_log_time
			, is_successful = false
			, error_log = v_error_msg 
		where id = v_log_id ;
	
		RAISE NOTICE 'Error occurred: %', v_error_msg;

	
end;
$$
language plpgsql; 

-- 
call fact_report_monthly_prc(202305);



-- index 
CREATE INDEX fact_txn_month_account_code_annalysis_code_trans_date_idx ON public.fact_txn_month (account_code,analysis_code,transaction_date);
CREATE INDEX fact_kpi_month_pos_city_monthkey_idx ON public.fact_kpi_month (pos_city,kpi_month);
CREATE INDEX fact_kpi_month_kpi_month_maxbucket_poscity_idx ON public.fact_kpi_month (kpi_month,pos_city,max_bucket);
CREATE INDEX fact_kpi_month_kpi_month_maxbucket_idx ON public.fact_kpi_month (kpi_month,max_bucket);
CREATE INDEX fact_kpi_month_pos_city_writeoff_month_idx ON public.fact_kpi_month (pos_city,write_off_month);


-- report 1 
select d.funding_name 
	, f.tpb_head as "Head"
	, f.tpb_mienbac as "Miền Bắc"
	, f.tpb_miennam as "Miền Nam"
	, f.tpb_mientrung as "Miền Trung"
	, f.tpv_total as "Total"
	, f.kvml_dbb as "Đông Bắc Bộ"
	, f.kvml_tbb as "Tây Bắc Bộ"
	, f.kvml_dbsh as "ĐB Sông Hồng"
	, f.kvml_btb as "Bắc Trung Bộ"
	, f.kvml_ntb as "Nam Trung Bộ"
	, f.kvml_tnb as "Tây Nam Bộ"
	, f.kvml_dnb as "Đông Nam Bộ"
	, f.kvml_total as "Total"
	, f.month_key as "Month"
from dim_funding_structure d 
join fact_backdate_funding_monthly f 
on d.funding_id = f.funding_id 
and f.month_key = 202302
order by d.sortorder ;

-- report 2
select 
	f.month_key 
	, f.area_cde 
	, f.email 
	, f.tongdiem as "Tổng điểm"
	, f.rank_final 
	, f.ltn_avg 
	, f.rank_ltn_avg 
	, f.psdn_avg 
	, f.rank_psdn_avg 
	, f.approval_rate_avg 
	, f.rank_approval_rate_avg 
	, f.npl_truoc_wo_luy_ke 
	, f.rank_npl_truoc_wo_luy_ke 
	, f.diem_quy_mo as "Điểm Quy Mô"
	, f.rank_ptkd 
	, f.cir 
	, f.rank_cir 
	, f.margin 
	, f.rank_margin 
	, f.hs_von 
	, f.rank_hs_von 
	, f.hsbq_nhan_su 
	, f.rank_hsbq_nhan_su 
	, f.diem_fin as "Điểm FIN"
	, f.rank_fin
from fact_backdate_asm_monthly f
where month_key = 202305
order by f.rank_final ;


--
select * from fact_backdate_asm_monthly 
truncate table fact_backdate_asm_monthly