import psycopg2
import pandas as pd
from psycopg2 import Error
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter


def export_postgres_to_excel(db_params, query, output_file):
    """
    Export data from PostgreSQL to an Excel file with custom formatting.
    Divide 'Head', 'Miền Bắc', 'Miền Trung', 'Miền Nam' columns by 1,000,000 and apply custom number format.

    Parameters:
    - db_params (dict): Database connection parameters (host, port, dbname, user, password).
    - query (str): SQL query to fetch data.
    - output_file (str): Path to the output Excel file.
    """
    connection = None
    cursor = None
    writer = None
    try:
        # Step 1: Connect to PostgreSQL
        print("Connecting to PostgreSQL...")
        connection = psycopg2.connect(
            host=db_params.get("host", "localhost"),
            port=db_params.get("port", "5432"),
            database=db_params.get("dbname"),
            user=db_params.get("user"),
            password=db_params.get("password"),
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

        # Step 4.1: Divide specified columns by 1,000,000
        columns_to_divide = ["Head", "Miền Bắc", "Miền Trung", "Miền Nam"]
        for col in columns_to_divide:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce") / 1000000
            else:
                print(f"Warning: '{col}' column not found in query results.")

        # Step 4.2: Add blank column before "Đông Bắc Bộ"
        if "Đông Bắc Bộ" in df.columns:
            blank_column_index = df.columns.get_loc("Đông Bắc Bộ")
            df.insert(blank_column_index, "", "")  # Empty header and values
        else:
            print(
                "Warning: 'Đông Bắc Bộ' column not found. Skipping blank column insertion."
            )

        # Step 5: Create Excel writer
        writer = pd.ExcelWriter(output_file, engine="openpyxl")
        df.to_excel(
            writer, sheet_name="Report", index=False, startrow=1
        )  # Start from row 2

        # Get workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets["Report"]

        # Define styles
        header_font = Font(name="Calibri", size=12, bold=True)
        cell_font = Font(name="Calibri", size=11)
        header_fill = PatternFill(
            start_color="ADD8E6", end_color="ADD8E6", fill_type="solid"
        )
        yellow_fill = PatternFill(
            start_color="FFFF00", end_color="FFFF00", fill_type="solid"
        )
        border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )
        center_alignment = Alignment(horizontal="center", vertical="center")
        left_alignment = Alignment(horizontal="left", vertical="center")
        right_alignment = Alignment(horizontal="right", vertical="center")
        group_header_font = Font(name="Calibri", size=14, bold=True)
        group_header_fill = PatternFill(
            start_color="D3D3D3", end_color="D3D3D3", fill_type="solid"
        )

        # Step 6: Format the column headers
        for col_num, column_title in enumerate(df.columns, 1):
            cell = worksheet.cell(row=2, column=col_num)
            cell.value = column_title
            cell.font = header_font
            cell.border = border
            cell.alignment = center_alignment
            cell.fill = yellow_fill if col_num == 7 else header_fill

        # Step 7: Format data rows
        formatted_columns = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_divide
            if col in df.columns
        }
        for row_num in range(3, len(df) + 3):  # Data starts from row 3
            for col_num in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row_num, column=col_num)
                cell.font = cell_font
                cell.border = border
                if col_num == 7:  # Column G (blank column)
                    cell.fill = yellow_fill
                if (
                    col_num in formatted_columns.values()
                ):  # Apply number format to specified columns
                    cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'

                # Align columns
                column_name = df.columns[col_num - 1]
                if column_name in [
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
            if col_num == 7:  # Column G
                worksheet.column_dimensions[column_letter].width = 3
            else:
                max_length = max(
                    max(
                        (len(str(val)) for val in df[column] if pd.notnull(val)),
                        default=10,
                    ),
                    len(column) if column else 10,
                )
                worksheet.column_dimensions[column_letter].width = min(
                    max_length + 2, 50
                )

        # Step 9: Set row heights
        worksheet.row_dimensions[1].height = 20
        worksheet.row_dimensions[2].height = 30
        for row_num in range(3, len(df) + 3):
            worksheet.row_dimensions[row_num].height = 20

        # Step 10: Add merged headers
        total_header_cell = worksheet.cell(row=1, column=2)
        total_header_cell.value = "Tổng cần phân bổ xuống cho ĐVML"
        total_header_cell.font = group_header_font
        total_header_cell.fill = group_header_fill
        total_header_cell.alignment = center_alignment
        total_header_cell.border = border
        worksheet.merge_cells(start_row=1, start_column=2, end_row=1, end_column=6)

        khu_vuc_header_cell = worksheet.cell(row=1, column=8)
        khu_vuc_header_cell.value = "KHU VỰC MẠNG LƯỚI"
        khu_vuc_header_cell.font = group_header_font
        khu_vuc_header_cell.fill = group_header_fill
        khu_vuc_header_cell.alignment = center_alignment
        khu_vuc_header_cell.border = border
        worksheet.merge_cells(start_row=1, start_column=8, end_row=1, end_column=14)

        # Step 11: Save the file
        writer.close()
        writer = None
        print(f"Data successfully exported to {output_file}")

    except Error as db_error:
        print(f"Database Error: {db_error}")
    except Exception as error:
        print(f"General Error: {error}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("PostgreSQL connection closed.")
        if writer:
            writer.close()


# Example usage
if __name__ == "__main__":
    db_params = {
        "host": "localhost",
        "port": "5432",
        "dbname": "final_project",
        "user": "postgres",
        "password": "1234",
    }

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

    output_file = "output_data3.xlsx"
    export_postgres_to_excel(db_params, query, output_file)
