Báo cáo Xếp hạng Kinh doanh
1. Ngữ cảnh
Nhận yêu cầu của phòng tài chính để làm 2 báo cáo này và chạy số hằng ngày, ghi kết quả vào file Excel trên Google Drive.

2. Thực hiện
Flowchart:
![flowchart_process](flowchart_report.png)
Mô tả từng bước thực hiện:

2.1.  Luồng Input Data:
* Sử dụng DBeaver để import dữ liệu từ 3 file Excel vào 3 bảng: fact_kpi_month, fact_txn_month, và kpi_asm_data.

2.2.  Kiểm tra Dữ liệu:
* Viết script SQL để kiểm tra tính chính xác của dữ liệu đã import.

2.3.  Tổ chức Mô hình Dữ liệu (Dimension & Fact):
* Mục tiêu: Lưu trữ tối ưu.
* Báo cáo tổng hợp:
* dim_funding_structure
* fact_backdate_funding_monthly
* Báo cáo xếp hạng ASM:
* fact_backdate_asm_monthly

2.4.  Tạo Bảng log_tracking:
* Ghi nhận thời gian bắt đầu, kết thúc và các lỗi (nếu có) trong quá trình xử lý.

2.5.  Tạo Bảng area_mapping:
* Mapping (ánh xạ) area_code với danh sách tỉnh tương ứng.

2.6.  Viết Stored Procedure:
* Khi truyền tham số tháng cần chạy lại:
* Xóa dữ liệu của tháng đó trong các bảng liên quan.
* Đổ dữ liệu mới đã xử lý vào.
* Dựng câu query SQL để truy vấn số liệu theo định dạng yêu cầu của 2 sheet báo cáo.

2.7.  Đánh Index:
* Tạo index cho các cột quan trọng từ dữ liệu đầu vào để giúp Stored Procedure chạy nhanh hơn.

2.8.  Dựng Câu Query cho Sheet:
* Xây dựng các câu query SQL để truy vấn số liệu theo đúng định dạng của từng sheet trong file Excel kết quả.

2.9.  Thực thi bằng Python:
* Sử dụng Python để thực thi các câu query SQL.
* Ghi kết quả ra file Excel.
* Điều chỉnh định dạng cho file Excel (font, màu sắc, độ rộng cột,...).

2.10. Upload lên Google Drive:
* Tự động tải file Excel đã hoàn thiện lên Google Drive.



