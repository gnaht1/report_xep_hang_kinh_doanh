import psycopg2
import pandas as pd
from psycopg2 import Error
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter
import numpy as np
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

        # --- CLEAR values at DataFrame index 27 for all columns except the first one ---
        # Index 27 corresponds to the 28th row (0-based). Leave the first column intact.
        if 27 in df.index:
            for col in df.columns[1:]:
                # Cast to object dtype first to avoid dtype mismatch when setting empty string
                df[col] = df[col].astype(object)
                df.at[27, col] = ""  # Clear the cell content

        # --- CLEAR values from DataFrame at indices 26–31 for columns "Head" through "Miền Trung" ---
        cols_to_clear = ["Head", "Miền Bắc", "Miền Nam", "Miền Trung"]
        for idx in [26, 27, 28, 29, 30, 31]:
            if idx in df.index:
                for col in cols_to_clear:
                    if col in df.columns:
                        df.at[idx, col] = ""

        # --- If indices 28–30 exist and there's a column "TOTAL" (uppercase), divide by 100 ---
        if "TOTAL" in df.columns:
            for idx in [28, 29, 30]:
                if idx in df.index:
                    raw = pd.to_numeric(df.loc[idx, "TOTAL"], errors="coerce")
                    df.loc[idx, "TOTAL"] = raw / 100

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
            "TOTAL",  # Note: "TOTAL" (uppercase) is different from "Total" (capital T)
        ]
        # All numeric columns for index 31 (divide by 1,000,000 as before)
        columns_to_divide_index_31 = (
            columns_to_divide_existing + ["Total"] + columns_to_divide_new
        )

        # === Step 4.1: Divide numeric values by 1,000,000 where appropriate ===
        # Existing region columns: skip DataFrame indices 28–31
        mask_existing = ~df.index.isin(range(28, 32))
        for col in columns_to_divide_existing:
            if col in df.columns:
                df.loc[mask_existing, col] = (
                    pd.to_numeric(df.loc[mask_existing, col], errors="coerce")
                    / 1_000_000
                ).round(2)
            else:
                print(f"Warning: '{col}' column not found in query results.")

        # Handle "Total" (capital T): skip DataFrame indices 26, 28–31
        if "Total" in df.columns:
            mask_total = ~df.index.isin([26] + list(range(28, 32)))
            df.loc[mask_total, "Total"] = (
                pd.to_numeric(df.loc[mask_total, "Total"], errors="coerce") / 1_000_000
            ).round(2)
        else:
            print("Warning: 'Total' column not found in query results.")

        # New region columns: skip DataFrame indices 26, 28–31
        rows_to_divide_new = [
            i for i in range(len(df)) if i not in [26, 28, 29, 30, 31]
        ]
        for col in columns_to_divide_new:
            if col in df.columns and rows_to_divide_new:
                df.loc[rows_to_divide_new, col] = (
                    pd.to_numeric(df.loc[rows_to_divide_new, col], errors="coerce")
                    / 1_000_000
                ).round(2)
            elif col not in df.columns:
                print(f"Warning: '{col}' column not found in query results.")

        # === Step 4.1.i: For DataFrame index 28, round selected columns to 2 decimals ===
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

        # === Step 4.1.ii: For DataFrame index 29, round selected columns to 1 decimal (no division) ===
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

        # === Step 4.1.iii: For DataFrame index 30, round selected columns to 1 decimal (no division) ===
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

        # === Step 4.1.iv: For DataFrame index 31, divide by 1,000,000 and round to 2 decimals ===
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

        # === Step 4.2: Add a blank column before "Đông Bắc Bộ" ===
        if "Đông Bắc Bộ" in df.columns:
            blank_column_index = df.columns.get_loc("Đông Bắc Bộ")
            df.insert(
                blank_column_index, "", ""
            )  # Column name is empty string, values are empty string
        else:
            print(
                "Warning: 'Đông Bắc Bộ' column not found. Skipping blank column insertion."
            )

        # === Step 5: Create Excel writer and write DataFrame ===
        writer = pd.ExcelWriter(output_file, engine="openpyxl")
        # Start data from row 2 (Excel row 3), header is in row 1 (Excel row 2)
        df.to_excel(writer, sheet_name="Report", index=False, startrow=1)

        workbook = writer.book
        worksheet = writer.sheets["Report"]

        # === Define cell styles ===
        header_font = Font(name="Calibri", size=12, bold=True, color="000000")
        cell_font = Font(name="Calibri", size=11, color="000000")

        header_fill = PatternFill(
            start_color="BDCFEF", end_color="BDCFEF", fill_type="solid"
        )
        yellow_fill = PatternFill(
            start_color="FFFF00", end_color="FFFF00", fill_type="solid"
        )
        green_fill = PatternFill(
            start_color="00FF00", end_color="00FF00", fill_type="solid"
        )
        bold_first_col_font = Font(name="Calibri", size=11, bold=True, color="000000")

        highlight0_font = Font(name="Calibri", size=11, bold=True, color="000000")
        highlight0_fill = PatternFill(
            start_color="6495ED", end_color="6495ED", fill_type="solid"
        )

        fill_26 = PatternFill(
            start_color="E3B825", end_color="E3B825", fill_type="solid"
        )
        fill_27 = PatternFill(
            start_color="F07A17", end_color="F07A17", fill_type="solid"
        )
        bold_row_font = Font(name="Calibri", size=11, bold=True, color="000000")

        border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )
        center_alignment = Alignment(horizontal="center", vertical="center")
        left_alignment = Alignment(horizontal="left", vertical="center")
        right_alignment = Alignment(horizontal="right", vertical="center")

        group_header_font = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
        group_header_fill = PatternFill(
            start_color="0000FF", end_color="0000FF", fill_type="solid"
        )

        # --- NEW Styles for specific rows/columns B-N ---
        special_rows_fill = PatternFill(
            start_color="A7FCF9", end_color="A7FCF9", fill_type="solid"
        )
        # Using existing bold_row_font as it matches the "bold text" requirement
        special_rows_number_format = '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'

        # === Step 6: Format column headers (row 2) ===
        for col_num, column_title in enumerate(df.columns, start=1):
            cell = worksheet.cell(row=2, column=col_num)
            if (
                column_title == "funding_name"
            ):  # Check if it's the original first column name from query
                cell.value = ""  # Set header for 'funding_name' to blank
            elif column_title == "" and col_num == (
                df.columns.get_loc("Đông Bắc Bộ") if "Đông Bắc Bộ" in df.columns else -1
            ):  # Check for the inserted blank column header
                cell.value = ""  # Ensure header of inserted blank column is also blank
            else:
                cell.value = column_title

            cell.font = header_font
            cell.border = border
            cell.alignment = center_alignment

            # Highlight the blank column header (inserted before "Đông Bắc Bộ")
            # The blank column's original name is "", find its actual position
            blank_col_actual_idx = -1
            if "" in df.columns:  # Check if blank column was successfully inserted
                # Find the first occurrence of an empty string column name
                try:
                    blank_col_actual_idx = df.columns.to_list().index("") + 1
                except ValueError:
                    pass  # Should not happen if inserted

            if col_num == blank_col_actual_idx:
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

        formatted_rows_new = [i + 3 for i in rows_to_divide_new]

        # === Step 8: Format data cells (rows start at Excel row 3) ===
        # DataFrame indices for special formatting (rows 1, 7, 12, 19, 20, 25 in 0-based df index)
        special_indices_for_formatting_A = {
            1,
            7,
            12,
            19,
            20,
            25,
        }  # For green fill in Col A
        special_indices_for_formatting_B_N = {
            1,
            7,
            12,
            19,
            20,
            25,
        }  # For new A7FCF9 fill in B-N

        # Get the actual column index of the inserted blank column if it exists
        blank_col_actual_idx_data = -1
        if "" in df.columns:
            try:
                blank_col_actual_idx_data = df.columns.to_list().index("") + 1
            except ValueError:
                pass

        for row_num in range(
            3, len(df) + 3
        ):  # Excel rows are 1-based, df starts at row 0
            df_index = row_num - 3  # Corresponding DataFrame index (0 to len(df)-1)
            for col_num in range(1, len(df.columns) + 1):  # Excel columns are 1-based
                cell = worksheet.cell(row=row_num, column=col_num)
                column_name = df.columns[col_num - 1]  # Get column name from DataFrame

                # --- 0) Formatting for DataFrame index 0 (Excel data row 1), columns B->N ---
                if df_index == 0 and 2 <= col_num <= 14:  # Columns B to N
                    if not (
                        cell.fill and cell.fill.fill_type
                    ):  # Check if cell already has a fill
                        cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)'
                        cell.fill = PatternFill(
                            start_color="F68216", end_color="F68216", fill_type="solid"
                        )
                        cell.font = Font(
                            name="Calibri", size=11, bold=True, color="FFFFFF"
                        )
                        cell.alignment = right_alignment
                        cell.border = border  # Apply border here as well
                    continue  # This formatting is exclusive for these cells

                # --- NEW: Formatting for special_indices (1, 7, 12, 19, 20, 25) for columns B-N ---
                if (
                    df_index in special_indices_for_formatting_B_N
                    and 2 <= col_num <= 14
                ):  # Columns B to N
                    cell.fill = special_rows_fill  # #a7fcf9
                    cell.font = bold_row_font  # Bold text
                    cell.number_format = special_rows_number_format  # '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'
                    cell.alignment = right_alignment
                    cell.border = border
                    continue  # This formatting takes precedence for these cells

                # --- 1) Format column B (Excel column 2) with specific integer format (if not handled by above) ---
                if (
                    col_num == 2
                ):  # This applies if not (df_index==0 and col_num==2) and not (df_index in special_indices_for_formatting_B_N and col_num==2)
                    cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)'
                    cell.alignment = right_alignment
                    cell.font = (
                        cell_font  # Default font if not a special row/column combo
                    )
                    cell.border = border
                    continue

                # --- 2) Highlight DataFrame index 0, first column (Column A) ---
                if df_index == 0 and col_num == 1:
                    cell.font = highlight0_font
                    cell.fill = highlight0_fill
                    cell.border = border
                    cell.alignment = left_alignment
                    continue

                # --- 3) Highlight first column (Column A) for specified DataFrame indices with green background ---
                if df_index in special_indices_for_formatting_A and col_num == 1:
                    cell.font = bold_first_col_font
                    cell.fill = green_fill
                    cell.border = border
                    cell.alignment = left_alignment
                    continue

                # Apply default font and border for cells not caught by highly specific rules above
                cell.font = cell_font
                cell.border = border

                # Highlight the blank data column (inserted before "Đông Bắc Bộ") with yellow fill
                if col_num == blank_col_actual_idx_data:
                    cell.fill = yellow_fill
                    # No continue here, as other formatting like number_format might still apply if it were not blank

                # --- Further column-specific and row-specific formatting ---

                # 1) "Head" column: integer format except indices 28–31 (df_index)
                if column_name == "Head":
                    if df_index in [28, 29, 30, 31]:  # df_index for these rows
                        cell.number_format = numbers.FORMAT_GENERAL
                    else:
                        cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)'
                    cell.alignment = right_alignment
                    # Fall through for potential row-specific fills (26, 27)

                # 2) "Total" (capital T) for df indices 0–25: integer format
                elif column_name == "Total" and df_index <= 25:
                    cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)'
                    cell.alignment = right_alignment
                    # Fall through

                # 3) Existing region columns (Miền Bắc, Miền Nam, Miền Trung, and "Total" for certain df_indices)
                elif col_num in cols_idx_existing:  # Check actual Excel column number
                    current_df_col_name = df.columns[
                        col_num - 1
                    ]  # Get df column name for logic
                    if current_df_col_name == "Total":
                        if df_index == 26:
                            cell.number_format = (
                                '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)'
                            )
                        elif df_index == 27:
                            cell.number_format = (
                                '_(* #,##0.00_);_(* (#,##0.00);_(* "-"_);_(@_)'
                            )
                        elif df_index in [28, 29, 30, 31]:
                            cell.number_format = (
                                numbers.FORMAT_GENERAL
                            )  # Already handled by division logic in df
                        # else: # Covered by rule 2 for Total <=25
                        #    pass
                    # For Miền Bắc, Miền Nam, Miền Trung (excluding Total) in existing columns
                    elif current_df_col_name in ["Miền Bắc", "Miền Nam", "Miền Trung"]:
                        if df_index in [28, 29, 30, 31]:
                            cell.number_format = numbers.FORMAT_GENERAL
                        else:
                            cell.number_format = (
                                '_(* #,##0.00_);_(* (#,##0.00);_(* "-"_);_(@_)'
                            )
                    cell.alignment = right_alignment
                    # Fall through

                # 4) Column "TOTAL" (uppercase) for df_index in [28, 29, 30]: integer format (after /100 division)
                elif column_name == "TOTAL" and df_index in [28, 29, 30]:
                    cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)'  # Values are already /100
                    cell.alignment = right_alignment
                    # Fall through

                # 5) New region columns (Đông Bắc Bộ, Tây Bắc Bộ, …) for indices != 26, 28, 29, 30, 31
                elif col_num in cols_idx_new and df_index not in [
                    26,
                    28,
                    29,
                    30,
                    31,
                ]:  # row_num in formatted_rows_new checks Excel row
                    # This rule applies to rows that underwent division by 1,000,000
                    if column_name == "TOTAL":  # Uppercase TOTAL in new columns
                        cell.number_format = '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)'
                    else:  # Other new region columns
                        cell.number_format = (
                            '_(* #,##0.00_);_(* (#,##0.00);_(* "-"_);_(@_)'
                        )
                    cell.alignment = right_alignment
                    # Fall through

                # 6) DataFrame index 28 (Excel row 31): two-decimal special for numeric columns
                elif (
                    df_index == 28 and col_num in cols_idx_28
                ):  # Excel row is df_index + 3
                    cell.number_format = (
                        '_(* #,##0.00_);_(* -#,##0.00_);_(* "-"_);_(@_)'
                    )
                    cell.alignment = right_alignment
                    # Fall through

                # 7) DataFrame index 29 (Excel row 32): one-decimal with minus sign for numeric columns
                elif df_index == 29 and col_num in cols_idx_29:
                    cell.number_format = '_(* #,##0.0_);_(* -#,##0.0_);_(* "-"_);_(@_)'
                    cell.alignment = right_alignment
                    # Fall through

                # 8) DataFrame index 30 (Excel row 33): one-decimal with minus sign for numeric columns
                elif df_index == 30 and col_num in cols_idx_30:
                    cell.number_format = '_(* #,##0.0_);_(* -#,##0.0_);_(* "-"_);_(@_)'
                    cell.alignment = right_alignment
                    # Fall through

                # 9) DataFrame index 31 (Excel row 34): two-decimal number format for all numeric columns in columns_to_divide_index_31
                elif df_index == 31 and column_name in columns_to_divide_index_31:
                    cell.number_format = '_(* #,##0.00_);_(* (#,##0.00);_(* "-"_);_(@_)'
                    cell.alignment = right_alignment
                    # Fall through

                # 10) Default alignment for other cells not specifically aligned right by number formats
                else:
                    # If cell.alignment was not already set to right_alignment by a number format rule
                    # and it's not the blank column (which might be empty or contain non-numeric)
                    # and not the first column (which is left_alignment by default for text)
                    if (
                        cell.alignment != right_alignment
                        and col_num != 1
                        and col_num != blank_col_actual_idx_data
                    ):
                        # Check if the value is numeric before left-aligning, might be better to just let Excel decide or set specific for text
                        # For simplicity, if no specific right_alignment was set, assume left_alignment for text-like data.
                        cell.alignment = left_alignment

                # === Additional formatting for DataFrame index 26 ===
                # Apply background color #E3B825 and bold text except columns B->E (2-5) and any already-colored column (e.g., column 7).
                if df_index == 26:  # Excel row 29
                    # Columns B-E are col_num 2,3,4,5. Blank column is blank_col_actual_idx_data
                    if not (2 <= col_num <= 5 or col_num == blank_col_actual_idx_data):
                        # Ensure not to overwrite the new A7FCF9 fill if this row/col is part of that rule,
                        # however, special_indices_for_formatting_B_N does not include 26. So no conflict here.
                        cell.fill = fill_26
                        cell.font = bold_row_font  # Ensure text is bold

                # === Additional formatting for DataFrame index 27 ===
                if df_index == 27:  # Excel row 30
                    if not (2 <= col_num <= 5 or col_num == blank_col_actual_idx_data):
                        # special_indices_for_formatting_B_N does not include 27. So no conflict here.
                        cell.fill = fill_27
                        cell.font = bold_row_font

        # === Step 9: Adjust column widths ===
        for col_idx, column_title_for_width in enumerate(df.columns, start=1):
            column_letter = get_column_letter(col_idx)
            if (
                col_idx == blank_col_actual_idx_data
            ):  # Check if it's the inserted blank column
                worksheet.column_dimensions[column_letter].width = 3
            else:
                max_length = 0
                # Header length
                header_val = worksheet.cell(row=2, column=col_idx).value
                if header_val:
                    max_length = len(str(header_val))

                # Data length
                for i, cell_val in enumerate(df[column_title_for_width]):
                    if pd.notnull(cell_val):
                        # Consider formatted length for numbers if possible, or just raw string length
                        # For simplicity, using string length. Might need refinement for perfect fit with number formats.
                        cell_str_val = str(cell_val)
                        # If specific number formats are applied, their visual length can differ.
                        # E.g., _(* #,##0.00_); adds spacing.
                        # This basic auto-width might not be perfect for heavily formatted numbers.
                        max_length = max(max_length, len(cell_str_val))

                adjusted_width = (
                    max_length + 2 if max_length > 0 else 10
                )  # Default width if column is empty
                worksheet.column_dimensions[column_letter].width = min(
                    adjusted_width, 50
                )

        # === Step 10: Set row heights ===
        worksheet.row_dimensions[1].height = 20  # Merged header row
        worksheet.row_dimensions[2].height = 30  # Column titles row
        for i in range(3, len(df) + 3):  # Data rows
            worksheet.row_dimensions[i].height = 20

        # === Step 11: Add merged headers with blue background and white text ===
        # Assuming the blank column is column 7 (index 6 in df.columns AFTER insertion)
        # Find column indices for merging based on names, robust to blank column insertion

        # Determine start/end columns for merged headers
        # Group 1: "Tổng cần phân bổ xuống cho ĐVML" (Head to Total)
        col_head_idx = df.columns.get_loc("Head") + 1 if "Head" in df.columns else -1
        col_total_idx = df.columns.get_loc("Total") + 1 if "Total" in df.columns else -1

        if col_head_idx != -1 and col_total_idx != -1 and col_head_idx <= col_total_idx:
            total_header_cell = worksheet.cell(row=1, column=col_head_idx)
            total_header_cell.value = "Tổng cần phân bổ xuống cho ĐVML"
            total_header_cell.font = group_header_font
            total_header_cell.fill = group_header_fill
            total_header_cell.alignment = center_alignment
            total_header_cell.border = border
            worksheet.merge_cells(
                start_row=1,
                start_column=col_head_idx,
                end_row=1,
                end_column=col_total_idx,
            )
            # Apply border to all cells in merged range for consistency
            for c in range(col_head_idx, col_total_idx + 1):
                worksheet.cell(row=1, column=c).border = border

        # Group 2: "KHU VỰC MẠNG LƯỚI" (Đông Bắc Bộ to TOTAL)
        col_dbb_idx = (
            df.columns.get_loc("Đông Bắc Bộ") + 1 if "Đông Bắc Bộ" in df.columns else -1
        )
        col_kvml_total_idx = (
            df.columns.get_loc("TOTAL") + 1 if "TOTAL" in df.columns else -1
        )  # Uppercase TOTAL

        if (
            col_dbb_idx != -1
            and col_kvml_total_idx != -1
            and col_dbb_idx <= col_kvml_total_idx
        ):
            khu_vuc_header_cell = worksheet.cell(row=1, column=col_dbb_idx)
            khu_vuc_header_cell.value = "KHU VỰC MẠNG LƯỚI"
            khu_vuc_header_cell.font = group_header_font
            khu_vuc_header_cell.fill = group_header_fill
            khu_vuc_header_cell.alignment = center_alignment
            khu_vuc_header_cell.border = border
            worksheet.merge_cells(
                start_row=1,
                start_column=col_dbb_idx,
                end_row=1,
                end_column=col_kvml_total_idx,
            )
            for c in range(col_dbb_idx, col_kvml_total_idx + 1):
                worksheet.cell(row=1, column=c).border = border

        # Ensure the cell for the blank column in row 1 also has a border if it exists and is not part of merges
        if blank_col_actual_idx_data != -1:
            is_blank_col_in_merge1 = (
                (col_head_idx <= blank_col_actual_idx_data <= col_total_idx)
                if col_head_idx != -1 and col_total_idx != -1
                else False
            )
            is_blank_col_in_merge2 = (
                (col_dbb_idx <= blank_col_actual_idx_data <= col_kvml_total_idx)
                if col_dbb_idx != -1 and col_kvml_total_idx != -1
                else False
            )
            if not is_blank_col_in_merge1 and not is_blank_col_in_merge2:
                blank_header_cell_row1 = worksheet.cell(
                    row=1, column=blank_col_actual_idx_data
                )
                blank_header_cell_row1.border = border  # Apply border
                # It might also need a fill if it's supposed to look like other non-merged header parts
                # worksheet.cell(row=1, column=blank_col_actual_idx_data).fill = yellow_fill # or header_fill or specific color

        # === Step 12: Save the file ===
        writer.close()  # Use close() instead of save() for ExcelWriter
        writer = None  # Set to None after closing
        print(f"Data successfully exported to {output_file}")

    except Error as db_error:
        print(f"Database Error: {db_error}")
    except Exception as error:
        print(f"General Error: {error}")
        import traceback

        traceback.print_exc()  # Print full traceback for debugging
        # raise # Re-raise the exception if you want it to propagate
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("PostgreSQL connection closed.")
        # Ensure writer is closed if it was opened, even if df.to_excel failed
        if writer is not None:
            print(
                "Closing writer in finally block (this might indicate an earlier error)..."
            )
            writer.close()


if __name__ == "__main__":
    db_params = {
        "host": "localhost",
        "port": "5432",
        "dbname": "final_project",
        "user": "postgres",
        "password": "1234",  # Replace with your actual password
    }

    # Make sure your query returns columns named exactly as used in the formatting logic
    # e.g., "Head", "Miền Bắc", "Total", "Đông Bắc Bộ", "TOTAL"
    query = """
    select 
        d.funding_name, -- This will be the first column (A)
        f.tpb_head as "Head",
        f.tpb_mienbac as "Miền Bắc",
        f.tpb_miennam as "Miền Nam",
        f.tpb_mientrung as "Miền Trung",
        f.tpv_total as "Total", -- Capital T
        -- Blank column will be inserted by pandas here if "Đông Bắc Bộ" exists
        f.kvml_dbb as "Đông Bắc Bộ",
        f.kvml_tbb as "Tây Bắc Bộ",
        f.kvml_dbsh as "ĐB Sông Hồng",
        f.kvml_btb as "Bắc Trung Bộ",
        f.kvml_ntb as "Nam Trung Bộ",
        f.kvml_tnb as "Tây Nam Bộ",
        f.kvml_dnb as "Đông Nam Bộ",
        f.kvml_total as "TOTAL", -- Uppercase TOTAL
        f.month_key as "Month" -- This column is not explicitly formatted, will get defaults
    from dim_funding_structure d 
    join fact_backdate_funding_monthly f 
    on d.funding_id = f.funding_id 
    where f.month_key = 202302 -- Ensure this month_key exists and returns data
    order by d.sortorder ;
    """

    output_file = "output_data_formatted.xlsx"
    export_postgres_to_excel(db_params, query, output_file)
