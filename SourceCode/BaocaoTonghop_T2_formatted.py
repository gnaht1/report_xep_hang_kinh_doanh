import psycopg2
import pandas as pd
from psycopg2 import Error
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


def export_postgres_to_excel(db_params, query, output_file):
    """
    Export data from PostgreSQL to an Excel file with custom formatting.

    Parameters:
    - db_params (dict): Database connection parameters (host, port, dbname, user, password).
    - query (str): SQL query to fetch data.
    - output_file (str): Path to the output Excel file.
    """
    connection = None
    try:
        # Step 1: Connect to PostgreSQL
        print("Connecting to PostgreSQL...")
        connection = psycopg2.connect(
            host=db_params["host"],
            port=db_params["port"],
            database=db_params["dbname"],
            user=db_params["user"],
            password=db_params["password"],
        )
        cursor = connection.cursor()

        # Step 2: Execute the query
        print("Executing query...")
        cursor.execute(query)

        # Step 3: Fetch column names and data
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        # Step 4: Create a pandas DataFrame
        print("Loading data into DataFrame...")
        df = pd.DataFrame(data, columns=columns)

        # Step 4.1: Add blank column before "Đông Bắc Bộ" with empty header
        blank_column_index = df.columns.get_loc("Đông Bắc Bộ")
        df.insert(
            blank_column_index, "", ""
        )  # Insert blank column with empty header and empty strings

        # Step 5: Create Excel writer
        writer = pd.ExcelWriter(output_file, engine="openpyxl")
        df.to_excel(
            writer, sheet_name="Report", index=False, startrow=1
        )  # Start from row 2 for header

        # Get workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets["Report"]

        # Define styles
        header_font = Font(name="Calibri", size=12, bold=True)
        cell_font = Font(name="Calibri", size=11)
        header_fill = PatternFill(
            start_color="ADD8E6", end_color="ADD8E6", fill_type="solid"
        )  # Light blue for column headers
        alternate_fill = PatternFill(
            start_color="F5F5F5", end_color="F5F5F5", fill_type="solid"
        )  # Light gray for data rows
        yellow_fill = PatternFill(
            start_color="FFFF00", end_color="FFFF00", fill_type="solid"
        )  # Bold yellow for column G
        border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )
        center_alignment = Alignment(horizontal="center", vertical="center")
        left_alignment = Alignment(horizontal="left", vertical="center")
        right_alignment = Alignment(horizontal="right", vertical="center")
        # Styles for merged headers
        group_header_font = Font(name="Calibri", size=14, bold=True)
        group_header_fill = PatternFill(
            start_color="D3D3D3", end_color="D3D3D3", fill_type="solid"
        )  # Light gray for group headers

        # Step 6: Format the column headers
        for col_num, column_title in enumerate(df.columns, 1):
            cell = worksheet.cell(row=2, column=col_num)
            cell.value = column_title
            cell.font = header_font
            cell.border = border
            cell.alignment = center_alignment
            # Apply yellow fill to column G (blank column)
            if col_num == 7:  # Column G
                cell.fill = yellow_fill
            else:
                cell.fill = header_fill

        # Step 7: Format data rows
        for row_num in range(3, len(df) + 3):  # Data starts from row 3
            for col_num in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row_num, column=col_num)
                cell.font = cell_font
                cell.border = border
                # Apply yellow fill to column G, overriding alternate fill
                if col_num == 7:  # Column G
                    cell.fill = yellow_fill
                elif row_num % 2 == 0:
                    cell.fill = alternate_fill
                # Align columns
                if df.columns[col_num - 1] in [
                    "Head",
                    "Miền Bắc",
                    "Miền Nam",
                    "Miền Trung",
                    "Total",
                    "Đông Bắc Bộ",
                    "Tây Bắc Bộ",
                    "ĐB Sông Hồng",
                    "Bắc Trung Bộ",
                    "Nam Trung Bộ",
                    "Tây Nam Bộ",
                    "Đông Nam Bộ",
                ]:
                    cell.alignment = right_alignment
                else:
                    cell.alignment = left_alignment

        # Step 8: Adjust column widths
        for col_num, column in enumerate(df.columns, 1):
            column_letter = get_column_letter(col_num)
            max_length = max(
                max((len(str(val)) for val in df[column]), default=10),
                len(column) if column else 10,  # Handle empty column header
            )
            adjusted_width = min(max_length + 2, 50)  # Max width 50
            worksheet.column_dimensions[column_letter].width = adjusted_width

        # Step 9: Set row heights
        worksheet.row_dimensions[1].height = 20  # Group header row
        worksheet.row_dimensions[2].height = 30  # Column header row
        for row_num in range(3, len(df) + 3):
            worksheet.row_dimensions[row_num].height = 20  # Data rows

        # Step 10: Add merged headers
        # Header for "TỔNG CẦN PHÂN BỔ XUỐNG CHO ĐVML" (columns B to F, i.e., 2 to 6)
        total_header_cell = worksheet.cell(row=1, column=2)
        total_header_cell.value = "Tổng cần phân bổ xuống cho ĐVML"
        total_header_cell.font = group_header_font
        total_header_cell.fill = group_header_fill
        total_header_cell.alignment = center_alignment
        total_header_cell.border = border
        worksheet.merge_cells(start_row=1, start_column=2, end_row=1, end_column=6)

        # Header for "KHU VỰC MẠNG LƯỚI" (columns H to N, i.e., 8 to 14)
        khu_vuc_header_cell = worksheet.cell(row=1, column=8)
        khu_vuc_header_cell.value = "KHU VỰC MẠNG LƯỚI"
        khu_vuc_header_cell.font = group_header_font
        khu_vuc_header_cell.fill = group_header_fill
        khu_vuc_header_cell.alignment = center_alignment
        khu_vuc_header_cell.border = border
        worksheet.merge_cells(start_row=1, start_column=8, end_row=1, end_column=14)

        # Step 11: Save the file
        writer.close()
        print(f"Data successfully exported to {output_file}")

    except (Exception, Error) as error:
        print(f"Error: {error}")

    finally:
        # Step 12: Close database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")


# Example usage
if __name__ == "__main__":
    # Database connection parameters
    db_params = {
        "host": "localhost",
        "port": "5432",
        "dbname": "final_project",
        "user": "postgres",
        "password": "1234",
    }

    # SQL query
    query = """
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
    """

    # Output Excel file path
    output_file = "output_data.xlsx"

    # Call the function
    export_postgres_to_excel(db_params, query, output_file)
