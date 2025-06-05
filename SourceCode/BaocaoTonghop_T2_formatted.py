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

        # Lists of columns for dividing and formatting
        columns_to_divide_existing = ["Head", "Miền Bắc", "Miền Nam", "Miền Trung"]
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
        # All numeric columns for index 31
        columns_to_divide_index_31 = (
            columns_to_divide_existing + ["Total"] + columns_to_divide_new
        )

        # === Step 4.1: Divide by 1,000,000 where appropriate ===
        # Existing columns: skip df indices 28–31
        mask_existing = ~df.index.isin(range(28, 32))
        for col in columns_to_divide_existing:
            if col in df.columns:
                df.loc[mask_existing, col] = (
                    pd.to_numeric(df.loc[mask_existing, col], errors="coerce")
                    / 1_000_000
                ).round(2)
            else:
                print(f"Warning: '{col}' column not found in query results.")

        # Handle 'Total': skip df indices 26, 28–31
        if "Total" in df.columns:
            mask_total = ~df.index.isin([26] + list(range(28, 32)))
            df.loc[mask_total, "Total"] = (
                pd.to_numeric(df.loc[mask_total, "Total"], errors="coerce") / 1_000_000
            ).round(2)
        else:
            print("Warning: 'Total' column not found in query results.")

        # New columns: skip df indices 26, 28–31
        rows_to_divide_new = [0, 1] + list(range(2, 6)) + list(range(6, 28))
        rows_to_divide_new = [i for i in rows_to_divide_new if i != 26]
        valid_rows_new = [i for i in rows_to_divide_new if i < len(df)]
        for col in columns_to_divide_new:
            if col in df.columns and valid_rows_new:
                df.loc[valid_rows_new, col] = (
                    pd.to_numeric(df.loc[valid_rows_new, col], errors="coerce")
                    / 1_000_000
                ).round(2)
            elif col not in df.columns:
                print(f"Warning: '{col}' column not found in query results.")

        # === Step 4.1.i: For df index 28, round selected columns to 2 decimals ===
        columns_to_format_index_28 = (
            columns_to_divide_existing + ["Total"] + columns_to_divide_new
        )
        if 28 in df.index:
            for col in columns_to_format_index_28:
                if col in df.columns:
                    val = pd.to_numeric(df.loc[28, col], errors="coerce")
                    df.loc[28, col] = round(val, 2) if not pd.isna(val) else val
                else:
                    print(f"Warning: '{col}' column not found for index 28 formatting.")

        # === Step 4.1.ii: For df index 29, round to 1 decimal (no division) ===
        columns_to_format_index_29 = (
            columns_to_divide_existing + ["Total"] + columns_to_divide_new
        )
        if 29 in df.index:
            for col in columns_to_format_index_29:
                if col in df.columns:
                    val = pd.to_numeric(df.loc[29, col], errors="coerce")
                    df.loc[29, col] = round(val, 1) if not pd.isna(val) else val
                else:
                    print(f"Warning: '{col}' column not found for index 29 formatting.")

        # === Step 4.1.iii: For df index 30, round to 1 decimal (no division) ===
        columns_to_format_index_30 = (
            columns_to_divide_existing + ["Total"] + columns_to_divide_new
        )
        if 30 in df.index:
            for col in columns_to_format_index_30:
                if col in df.columns:
                    val = pd.to_numeric(df.loc[30, col], errors="coerce")
                    df.loc[30, col] = round(val, 1) if not pd.isna(val) else val
                else:
                    print(f"Warning: '{col}' column not found for index 30 formatting.")

        # === Step 4.1.iv: For df index 31, divide by 1,000,000 and round to 2 decimals ===
        if 31 in df.index:
            for col in columns_to_divide_index_31:
                if col in df.columns:
                    raw_val = pd.to_numeric(df.loc[31, col], errors="coerce")
                    df.loc[31, col] = (
                        round(raw_val / 1_000_000, 2)
                        if not pd.isna(raw_val)
                        else raw_val
                    )
                else:
                    print(f"Warning: '{col}' column not found for index 31 division.")

        # === Step 4.2: Add blank column before "Đông Bắc Bộ" ===
        if "Đông Bắc Bộ" in df.columns:
            blank_column_index = df.columns.get_loc("Đông Bắc Bộ")
            df.insert(blank_column_index, "", "")
        else:
            print(
                "Warning: 'Đông Bắc Bộ' column not found. Skipping blank column insertion."
            )

        # === Step 5: Create Excel writer and write DataFrame ===
        writer = pd.ExcelWriter(output_file, engine="openpyxl")
        df.to_excel(writer, sheet_name="Report", index=False, startrow=1)

        workbook = writer.book
        worksheet = writer.sheets["Report"]

        # === Define cell styles ===
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

        # === Step 6: Format column headers (row 2) ===
        for col_num, column_title in enumerate(df.columns, start=1):
            cell = worksheet.cell(row=2, column=col_num)
            cell.value = column_title
            cell.font = header_font
            cell.border = border
            cell.alignment = center_alignment
            # Highlight the blank column (column 7 after insertion)
            if col_num == 7:
                cell.fill = yellow_fill
            else:
                cell.fill = header_fill

        # === Step 7: Determine column indices for formatting logic ===
        formatted_cols_existing = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_divide_existing
            if col in df.columns
        }
        formatted_cols_new = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_divide_new
            if col in df.columns
        }
        formatted_cols_28 = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_format_index_28
            if col in df.columns
        }
        formatted_cols_29 = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_format_index_29
            if col in df.columns
        }
        formatted_cols_30 = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_format_index_30
            if col in df.columns
        }
        formatted_cols_31 = {
            col: df.columns.get_loc(col) + 1
            for col in columns_to_divide_index_31
            if col in df.columns
        }

        total_idx_existing = formatted_cols_existing.get("Total")
        total_idx_new = formatted_cols_new.get("TOTAL")
        cols_idx_existing = list(formatted_cols_existing.values())
        cols_idx_new = list(formatted_cols_new.values())
        cols_idx_28 = list(formatted_cols_28.values())
        cols_idx_29 = list(formatted_cols_29.values())
        cols_idx_30 = list(formatted_cols_30.values())
        cols_idx_31 = list(formatted_cols_31.values())

        formatted_rows_new = [
            i + 3 for i in valid_rows_new
        ]  # Excel rows to format for new columns

        # === Step 8: Format data cells (rows start at Excel row 3) ===
        for row_num in range(3, len(df) + 3):
            for col_num in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row_num, column=col_num)
                cell.font = cell_font
                cell.border = border

                # Highlight the blank column (column 7)
                if col_num == 7:
                    cell.fill = yellow_fill

                column_name = df.columns[col_num - 1]
                df_index = row_num - 3  # corresponding DataFrame index

                # 1) "Head" column: integer format except indices 28–31
                if column_name == "Head":
                    if (
                        31 <= row_num <= 34
                    ):  # Excel rows 31–34 correspond to df indices 28–31
                        cell.number_format = numbers.FORMAT_GENERAL
                    else:
                        cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'
                    cell.alignment = right_alignment
                    continue

                # 2) "Total" for df indices 0–25: integer format
                if column_name == "Total" and df_index <= 25:
                    cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'
                    cell.alignment = right_alignment
                    continue

                # 3) Existing region columns (Miền Bắc, Miền Nam, Miền Trung, Total for df_index>25)
                if col_num in cols_idx_existing:
                    # For "Total" when df_index 26 or 27 → two-decimal format
                    if column_name == "Total" and (26 <= df_index <= 27):
                        cell.number_format = (
                            '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                        )
                    # df_index 26 → general
                    elif column_name == "Total" and df_index == 26:
                        cell.number_format = numbers.FORMAT_GENERAL
                    else:
                        # Other existing numeric cells: two-decimal unless index 28–31
                        if 31 <= row_num <= 34:
                            cell.number_format = numbers.FORMAT_GENERAL
                        else:
                            cell.number_format = (
                                '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                            )
                    cell.alignment = right_alignment
                    continue

                # 4) New region columns (Đông Bắc Bộ, Tây Bắc Bộ, …, TOTAL)
                # Only format here if not df_index == 31
                if col_num in cols_idx_new and df_index != 31:
                    if row_num in formatted_rows_new:
                        # For "TOTAL" column → integer format
                        if col_num == total_idx_new:
                            cell.number_format = (
                                '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'
                            )
                        else:
                            cell.number_format = (
                                '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                            )
                    else:
                        cell.number_format = numbers.FORMAT_GENERAL
                    cell.alignment = right_alignment
                    continue

                # 5) df index 28 (Excel row 31): two-decimal special
                if col_num in cols_idx_28 and row_num == 31:
                    cell.number_format = (
                        '_(* #,##0.00_);_(* -#,##0.00_);_(* "-"??_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    continue

                # 6) df index 29 (Excel row 32): one-decimal with minus sign
                if col_num in cols_idx_29 and row_num == 32:
                    cell.number_format = (
                        '_(* #,##0.0_);_(* -#,##0.0_);_(* "-"??_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    continue

                # 7) df index 30 (Excel row 33): one-decimal with minus sign
                if col_num in cols_idx_30 and row_num == 33:
                    cell.number_format = (
                        '_(* #,##0.0_);_(* -#,##0.0_);_(* "-"??_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    continue

                # 8) df index 31 (Excel row 34): two-decimal number format for all numeric columns
                if df_index == 31 and column_name in columns_to_divide_index_31:
                    cell.number_format = (
                        '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    continue

                # 9) Default alignment for other cells
                cell.alignment = left_alignment

        # === Step 9: Adjust column widths ===
        for col_num, column in enumerate(df.columns, start=1):
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

        # === Step 10: Set row heights ===
        worksheet.row_dimensions[1].height = 20
        worksheet.row_dimensions[2].height = 30
        for row_num in range(3, len(df) + 3):
            worksheet.row_dimensions[row_num].height = 20

        # === Step 11: Add merged headers ===
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

        # === Step 12: Save the file ===
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

    output_file = "output_data1.xlsx"
    export_postgres_to_excel(db_params, query, output_file)
