import psycopg2
import pandas as pd
from psycopg2 import Error


def export_postgres_to_excel(db_params, query, output_file):
    """
    Export data from PostgreSQL to an Excel file.

    Parameters:
    - db_params (dict): Database connection parameters (host, port, dbname, user, password).
    - query (str): SQL query to fetch data.
    - output_file (str): Path to the output Excel file.
    """
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

        # Step 5: Export to Excel
        print(f"Writing data to {output_file}...")
        df.to_excel(output_file, index=False, engine="openpyxl")

        print(f"Data successfully exported to {output_file}")

    except (Exception, Error) as error:
        print(f"Error: {error}")

    finally:
        # Step 6: Close database connection
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

    # SQL query (replace with your table/query)
    query = """
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
    where month_key = 202302
    order by f.rank_final ;
    
    
    """

    # Output Excel file path
    output_file = "output2_data.xlsx"

    # Call the function
    export_postgres_to_excel(db_params, query, output_file)
