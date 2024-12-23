import pandas as pd
import re
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

# Constants
FILE_PATH = r"D:\Drive\Learning\Python\ETL\Ex_Files_ETL_Python_SQL\Practice\H+ Sport Employees.xlsx"

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

# Environment variables for sensitive information (use a .env file or export in your terminal)
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

def clean_employee_name(name: str) -> str:
    """
    Cleans the employee name by removing commas, extra spaces, and non-ASCII characters.
    """
    name = re.sub(r'\s+', ' ', name)  # Replace multiple spaces with a single space
    name = re.sub(r'[^\x00-\x7F]+', '', name)  # Remove non-ASCII characters
    name = name.replace(',', '')  # Remove commas
    return name.strip()  # Remove leading/trailing spaces

def load_employee_data(filepath: str) -> pd.DataFrame:
    """
    Loads employee data from an Excel file and cleans up the 'Employee Name' column.
    """
    employees = pd.read_excel(filepath, sheet_name='Emp')
    
    # Clean 'Employee Name' column
    employees['Employee Name'] = employees['Employee Name'].apply(clean_employee_name)
    
    # Drop unnecessary columns
    columns_to_drop = ['Job Rating', 'New Salary', 'Tax Rate', '2.91%']
    employees.drop(columns=columns_to_drop, inplace=True)
    
    return employees

def connect_to_snowflake() -> snowflake.connector.connect:
    """
    Establishes and returns a connection to Snowflake using environment variables.
    """
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

def create_table_if_not_exists(cursor) -> None:
    """
    Creates the table in Snowflake if it doesn't already exist.
    """
    cursor.execute("""
        CREATE OR REPLACE TABLE Employees (
            "Employee Name" STRING,
            "Building" STRING,
            "Department" STRING,
            "Status" STRING,
            "Hire Date" DATE,
            "Month" STRING,
            "Years" INT,
            "Benefits" STRING,
            "Salary" NUMBER
        )
    """)
    cursor.connection.commit()

def insert_data_to_snowflake(conn, data: pd.DataFrame) -> None:
    """
    Inserts cleaned employee data into Snowflake using the write_pandas function.
    """
    write_pandas(conn, data, table_name='Employees', database=SNOWFLAKE_DATABASE, 
                 schema=SNOWFLAKE_SCHEMA, auto_create_table=True)

def main():
    try:
        # Load and clean employee data
        employees = load_employee_data(FILE_PATH)
        
        # Connect to Snowflake and execute operations
        with connect_to_snowflake() as conn:
            with conn.cursor() as cur:
                create_table_if_not_exists(cur)
                insert_data_to_snowflake(conn, employees)
        
        print("Data successfully inserted into Snowflake.")
    
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"SQL error occurred: {e}")
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
