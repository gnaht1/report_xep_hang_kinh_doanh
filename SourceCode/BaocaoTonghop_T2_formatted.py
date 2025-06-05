import psycopg2
import pandas as pd
from psycopg2 import Error
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter
import math


def export_postgres_to_excel(db_params, query, output_file):
    """
    Export data from PostgreSQL to an Excel file with custom formatting.
    Divide 'Head', 'Miền Bắc', 'Miền Trung', 'Miền Nam' by 1,000,000, except for Excel
    rows 31 to 34 (DataFrame indices 28 to 31). Divide 'Total' by 1,000,000, except for
    indices 26 and 28 to 31 (Excel rows 29 and 31 to 34). Apply custom number format with
    2 decimal places to these columns, except rows 31 to 34.
    Divide columns 'Đông Bắc Bộ' to 'Đông Nam Bộ' and 'TOTAL' by 1,000,000 and apply
    accounting format with 2 decimal places for 'Đông Bắc Bộ' to 'Đông Nam Bộ' and
    integer format for 'TOTAL' for DataFrame indices 0, 1, 2 to 5, 6 to 27, excluding
    index 26 (Excel rows 3, 4, 5 to 8, 9 to 30, excluding row 29).
    For DataFrame index 28 (Excel row 31), format columns 'Head' to 'Đông Nam Bộ' as
    float rounded to 2 decimal places, with 2-decimal-place accounting format in Excel
    using minus sign for negatives (e.g., -13.37).

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
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Step 2: Execute the query
        print("Executing query...")
        cursor.execute(query)

        # Step 3: Fetch column names and data
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        # Step 4: Create a pandas DataFrame
        print("Loading data into DataFrame...")
        print(f"Columns retrieved: {columns}")
        df = pd.DataFrame(data, columns=columns)
        if df.empty:
            print("Warning: Query returned no data. Creating empty Excel file.")
            df = pd.DataFrame(columns=columns)

        # Step 4.1: Divide specified columns by 1,000,000
        # Existing columns: Head, Miền Bắc, Miền Trung, Miền Nam, Total
        columns_to_divide_existing = [
            "Head",
            "Miền Bắc",
            "Miền Nam",
            "Miền Trung",
        ]
        mask_existing = ~df.index.isin(range(28, 32))  # Exclude indices 28 to 31
        for col in columns_to_divide_existing:
            if col in df.columns:
                df.loc[mask_existing, col] = (
                    pd.to_numeric(df.loc[mask_existing, col], errors="coerce") / 1000000
                ).round(2)  # Round to 2 decimal places
            else:
                print(f"Warning: '{col}' column not found in query results.")

        # Handle 'Total' column separately to exclude index 26
        if "Total" in df.columns:
            mask_total_except_26_and_28_to_31 = ~df.index.isin(
                [26] + list(range(28, 32))
            )  # Exclude index 26 and 28 to 31
            df.loc[mask_total_except_26_and_28_to_31, "Total"] = (
                pd.to_numeric(
                    df.loc[mask_total_except_26_and_28_to_31, "Total"], errors="coerce"
                )
                / 1000000
            ).round(2)  # Round to 2 decimal places
        else:
            print("Warning: 'Total' column not found in query results.")

        # New columns: Đông Bắc Bộ to Đông Nam Bộ and TOTAL
        columns_to_divide_new = [
            "Đông Bắc Bộ",
            "Tây Bắc Bộ",
            "ĐB Sông Hồng",
            "Bắc Trung Bộ",
            "Nam Trung Bộ",
            "Tây Nam Bộ",
            "Đông Nam Bộ",
            "TOTAL",
        ]
        rows_to_divide_new = (
            [0, 1] + list(range(2, 6)) + list(range(6, 28))
        )  # Indices 0, 1, 2 to 5, 6 to 27
        rows_to_divide_new = [
            i for i in rows_to_divide_new if i != 26
        ]  # Exclude index 26
        valid_rows = [i for i in rows_to_divide_new if i < len(df)]
        for col in columns_to_divide_new:
            if col in df.columns:
                if valid_rows:
                    df.loc[valid_rows, col] = (
                        pd.to_numeric(df.loc[valid_rows, col], errors="coerce")
                        / 1000000
                    ).round(2)  # Divide by 1,000,000 and round to 2 decimal places
            else:
                print(f"Warning: '{col}' column not found in query results.")

        # Format index 28 for columns Head to Đông Nam Bộ as float rounded to 2 decimal places
        columns_to_format_index_28 = [
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
        ]
        if 28 in df.index:
            for col in columns_to_format_index_28:
                if col in df.columns:
                    value = pd.to_numeric(df.loc[28, col], errors="coerce")
                    df.loc[28, col] = (
                        round(value, 2) if not pd.isna(value) else value
                    )  # Round to 2 decimal places, preserve NaN
                else:
                    print(f"Warning: '{col}' column not found for index 28 formatting.")

        # Debug: Print values for specified rows in new columns
        if valid_rows:
            print(
                "Values for DataFrame indices 0, 1, 2 to 5, 6 to 27, excluding 26 (Excel rows 3, 4, 5 to 8, 9 to 30, excluding 29):"
            )
            print(df.loc[valid_rows, columns_to_divide_new])

        # Debug: Print index 26 if it exists
        if 26 in df.index:
            print("Values for DataFrame index 26 (Excel row 29):")
            print(df.loc[[26], ["Total"] + columns_to_divide_new])

        # Debug: Print index 28 if it exists
        if 28 in df.index:
            print("Values for DataFrame index 28 (Excel row 31):")
            print(df.loc[[28], columns_to_format_index_28])

        # Step 4.2: Add blank column before "Đông Bắc Bộ"
        if "Đông Bắc Bộ" in df.columns:
            blank_column_index = df.columns.get_loc("Đông Bắc Bộ")
            df.insert(blank_column_index, "", "")
        else:
            print(
                "Warning: 'Đông Bắc Bộ' column not found. Skipping blank column insertion."
            )

        # Step 5: Create Excel writer
        writer = pd.ExcelWriter(output_file, engine="openpyxl")
        df.to_excel(writer, sheet_name="Report", index=False, startrow=1)

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
        formatted_columns_existing = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_divide_existing
            if col in df.columns
        }
        formatted_columns_new = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_divide_new
            if col in df.columns
        }
        formatted_columns_index_28 = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_format_index_28
            if col in df.columns
        }
        total_col_index = formatted_columns_existing.get("Total")
        total_upper_col_index = formatted_columns_new.get("TOTAL")
        formatted_col_indices_existing = list(formatted_columns_existing.values())
        formatted_col_indices_new = list(formatted_columns_new.values())
        formatted_col_indices_index_28 = list(formatted_columns_index_28.values())
        formatted_rows_new = [
            i + 3 for i in rows_to_divide_new
        ]  # Excel rows 3, 4, 5 to 8, 9 to 30, excluding 29
        for row_num in range(3, len(df) + 3):
            for col_num in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row_num, column=col_num)
                cell.font = cell_font
                cell.border = border
                if col_num == 7:
                    cell.fill = yellow_fill
                # Existing columns
                if col_num in formatted_col_indices_existing:
                    if 31 <= row_num <= 34:
                        cell.number_format = numbers.FORMAT_GENERAL
                    else:
                        cell.number_format = (
                            '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                        )
                # New columns
                elif col_num in formatted_col_indices_new:
                    if row_num in formatted_rows_new:
                        if col_num == total_upper_col_index:
                            cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'  # Integer format for TOTAL
                        else:
                            cell.number_format = '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'  # 2 decimal places
                    else:
                        cell.number_format = numbers.FORMAT_GENERAL
                # Index 28 columns (Head to Đông Nam Bộ)
                if col_num in formatted_col_indices_index_28 and row_num == 31:
                    cell.number_format = '_(* #,##0.00_);_(* -#,##0.00_);_(* "-"??_);_(@_)'  # 2 decimal places with minus sign for negatives
                # Align columns
                column_name = df.columns[col_num - 1]
                if column_name in [
                    "Head",
                    "Miền Bắc",
                    "Miền Nam",
                    "Miền Trung",
                    "Total",
                    "TOTAL",
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
            if col_num == 7:
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
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("PostgreSQL connection closed.")
        if writer:
            writer.close()


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
        , f.kvml_total as "TOTAL"
        , f.month_key as "Month"
    from dim_funding_structure d 
    join fact_backdate_funding_monthly f 
    on d.funding_id = f.funding_id 
    and f.month_key = 202302
    order by d.sortorder ;
    """

    output_file = "output_data3.xlsx"
    export_postgres_to_excel(db_params, query, output_file)
