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

        # Step 4.1: Divide specified columns by 1,000,000 (skip for indices 28–31)
        columns_to_divide_existing = [
            "Head",
            "Miền Bắc",
            "Miền Nam",
            "Miền Trung",
        ]
        # Exclude df indices 28, 29, 30, 31
        mask_existing = ~df.index.isin(range(28, 32))
        for col in columns_to_divide_existing:
            if col in df.columns:
                df.loc[mask_existing, col] = (
                    pd.to_numeric(df.loc[mask_existing, col], errors="coerce") / 1000000
                ).round(2)
            else:
                print(f"Warning: '{col}' column not found in query results.")

        # Handle 'Total' column separately: exclude index 26, 28–31
        if "Total" in df.columns:
            mask_total_except = ~df.index.isin([26] + list(range(28, 32)))
            df.loc[mask_total_except, "Total"] = (
                pd.to_numeric(df.loc[mask_total_except, "Total"], errors="coerce")
                / 1000000
            ).round(2)
        else:
            print("Warning: 'Total' column not found in query results.")

        # Step 4.1 (continued): Divide new columns by 1,000,000 (skip indices 26, 28–31)
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
        # Build list of indices to divide: 0,1,2–5,6–27 (skip 26); excludes 28+ so 29,30,31 are not divided
        rows_to_divide_new = [0, 1] + list(range(2, 6)) + list(range(6, 28))
        rows_to_divide_new = [i for i in rows_to_divide_new if i != 26]
        valid_rows = [i for i in rows_to_divide_new if i < len(df)]
        for col in columns_to_divide_new:
            if col in df.columns:
                if valid_rows:
                    df.loc[valid_rows, col] = (
                        pd.to_numeric(df.loc[valid_rows, col], errors="coerce")
                        / 1000000
                    ).round(2)
            else:
                print(f"Warning: '{col}' column not found in query results.")

        # Step 4.1 (continued): For df index 28, format as float rounded to 2 decimals
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
                    val = pd.to_numeric(df.loc[28, col], errors="coerce")
                    df.loc[28, col] = round(val, 2) if not pd.isna(val) else val
                else:
                    print(f"Warning: '{col}' column not found for index 28 formatting.")

        # Step 4.1 (continued): For df index 29, format all numeric columns as float rounded to 1 decimal (no division)
        columns_to_format_index_29 = [
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
            "TOTAL",
        ]
        if 29 in df.index:
            for col in columns_to_format_index_29:
                if col in df.columns:
                    val = pd.to_numeric(df.loc[29, col], errors="coerce")
                    df.loc[29, col] = round(val, 1) if not pd.isna(val) else val
                else:
                    print(f"Warning: '{col}' column not found for index 29 formatting.")

        # Step 4.1 (continued): For df index 30, format all numeric columns as float rounded to 1 decimal (no division)
        columns_to_format_index_30 = [
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
            "TOTAL",
        ]
        if 30 in df.index:
            for col in columns_to_format_index_30:
                if col in df.columns:
                    val = pd.to_numeric(df.loc[30, col], errors="coerce")
                    df.loc[30, col] = round(val, 1) if not pd.isna(val) else val
                else:
                    print(f"Warning: '{col}' column not found for index 30 formatting.")

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

        # Step 6: Format the column headers (row 2)
        for col_num, column_title in enumerate(df.columns, 1):
            cell = worksheet.cell(row=2, column=col_num)
            cell.value = column_title
            cell.font = header_font
            cell.border = border
            cell.alignment = center_alignment
            # Highlight the blank column (which is column 7 after insertion)
            if col_num == 7:
                cell.fill = yellow_fill
            else:
                cell.fill = header_fill

        # Step 7: Determine column indices for formatting
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
        formatted_columns_index_29 = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_format_index_29
            if col in df.columns
        }
        formatted_columns_index_30 = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_format_index_30
            if col in df.columns
        }
        total_col_index_existing = formatted_columns_existing.get("Total")
        total_col_index_new = formatted_columns_new.get("TOTAL")
        formatted_col_indices_existing = list(formatted_columns_existing.values())
        formatted_col_indices_new = list(formatted_columns_new.values())
        formatted_col_indices_index_28 = list(formatted_columns_index_28.values())
        formatted_col_indices_index_29 = list(formatted_columns_index_29.values())
        formatted_col_indices_index_30 = list(formatted_columns_index_30.values())

        # Rows (Excel) corresponding to df indices for new columns formatting
        formatted_rows_new = [
            i + 3 for i in rows_to_divide_new
        ]  # Excel rows 3.., excluding 29

        # Step 7: Format data rows (starting at Excel row 3)
        for row_num in range(3, len(df) + 3):
            for col_num in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row_num, column=col_num)
                cell.font = cell_font
                cell.border = border

                # Highlight the blank column (column 7)
                if col_num == 7:
                    cell.fill = yellow_fill

                column_name = df.columns[col_num - 1]
                df_index = row_num - 3  # df index corresponding to this Excel row

                # 1) Special formatting for “Head” (integer format) except indices 28–31
                if column_name == "Head":
                    if 31 <= row_num <= 34:
                        cell.number_format = numbers.FORMAT_GENERAL
                    else:
                        cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'
                    cell.alignment = right_alignment
                    continue

                # 2) Special formatting for “Total” on df indices 0→25
                if column_name == "Total" and df_index <= 25:
                    cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'
                    cell.alignment = right_alignment
                    continue

                # 3) Existing columns (Miền Bắc, Miền Nam, Miền Trung, Total for df_index>25)
                if col_num in formatted_col_indices_existing:
                    # For Total when df_index > 25:
                    if column_name == "Total" and (26 <= df_index <= 27):
                        # df_index 26 or 27 → two-decimal format
                        cell.number_format = (
                            '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                        )
                    elif column_name == "Total" and df_index == 26:
                        # df_index 26 → general
                        cell.number_format = numbers.FORMAT_GENERAL
                    else:
                        # Miền Bắc, Miền Nam, Miền Trung (all df indices except 28–31)
                        if 31 <= row_num <= 34:
                            cell.number_format = numbers.FORMAT_GENERAL
                        else:
                            cell.number_format = (
                                '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                            )
                    cell.alignment = right_alignment
                    continue

                # 4) New columns (Đông Bắc Bộ, Tây Bắc Bộ, …, TOTAL)
                if col_num in formatted_col_indices_new:
                    if row_num in formatted_rows_new:
                        if col_num == total_col_index_new:
                            # “TOTAL” column (integer for those rows)
                            cell.number_format = (
                                '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'
                            )
                        else:
                            # other new columns (2 decimals)
                            cell.number_format = (
                                '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                            )
                    else:
                        cell.number_format = numbers.FORMAT_GENERAL
                    cell.alignment = right_alignment
                    continue

                # 5) Index 28 special rounding for select columns (Excel row 31, df_index=28)
                if col_num in formatted_col_indices_index_28 and row_num == 31:
                    cell.number_format = (
                        '_(* #,##0.00_);_(* -#,##0.00_);_(* "-"??_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    continue

                # 6) Index 29 special rounding and one decimal with minus sign (Excel row 32, df_index=29)
                if col_num in formatted_col_indices_index_29 and row_num == 32:
                    # Use minus sign instead of parentheses, one decimal
                    cell.number_format = (
                        '_(* #,##0.0_);_(* -#,##0.0_);_(* "-"??_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    continue

                # 7) Index 30 special rounding and one decimal with minus sign (Excel row 33, df_index=30)
                if col_num in formatted_col_indices_index_30 and row_num == 33:
                    # Use minus sign instead of parentheses, one decimal
                    cell.number_format = (
                        '_(* #,##0.0_);_(* -#,##0.0_);_(* "-"??_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    continue

                # 8) Default alignment for other cells
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

    output_file = "output_data8.xlsx"
    export_postgres_to_excel(db_params, query, output_file)
