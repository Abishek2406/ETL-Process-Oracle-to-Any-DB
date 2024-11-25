import pandas as pd
import time
import os
import connection_db as c_db
import logging
import cx_Oracle
from sqlalchemy import text
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_connection(db_type: str, ints: int = None):
    try:
        if db_type == "Oracle":
            return c_db.db_connection.connect_oracle(ints)
        elif db_type == "Postgresql":
            return c_db.db_connection.connect_postgresql()
        elif db_type == "MySql":
            return c_db.db_connection.connect_mysql()
        elif db_type == "MariaDB":
            return c_db.db_connection.connect_mariadb()
        elif db_type == "MSSQL":
            return c_db.db_connection.connect_mssql()
        else:
            logging.error(f"Unsupported database type: {db_type}")
            return None
    except Exception as e:
        logging.error(f"Error connecting to {db_type}: {e}")
        return None

def log_etl_process(engine, process_name, status, details, start_time=None, end_time=None):
    try:
        query = text("""
            MERGE INTO etl_process_log target
            USING (SELECT :process_name AS process_name, :status AS status, :details AS details, 
                   TO_TIMESTAMP(TO_CHAR(:start_time,'RRRR-MM-DD HH24:MI:SS'), 'RRRR-MM-DD HH24:MI:SS') AS start_time,
                   TO_TIMESTAMP(TO_CHAR(:end_time,'RRRR-MM-DD HH24:MI:SS'), 'RRRR-MM-DD HH24:MI:SS') AS end_time FROM dual) source
            ON (target.process_name = source.process_name AND target.start_time = source.start_time)
            WHEN MATCHED THEN
                UPDATE SET status = source.status, details = source.details, end_time = source.end_time
            WHEN NOT MATCHED THEN
                INSERT (process_name, status, details, start_time, end_time)
                VALUES (source.process_name, source.status, source.details, source.start_time, source.end_time)
        """)

        # logging.info(f"Executing query: {query} with parameters: process_name={process_name}, status={status}, details={details}, start_time={start_time}, end_time={end_time}")

        with engine.connect() as connection:
            result = connection.execute(query, {
                'process_name': process_name,
                'status': status,
                'details': details,
                'start_time': start_time,
                'end_time': end_time
            })
            connection.commit() 
            logging.info(f"Rows affected: {result.rowcount}")
            
        logging.info(f"ETL process logged successfully: {process_name}, Status: {status}")

    except Exception as e:
        logging.error(f"Failed to log ETL process: {e}")

def get_columns_and_types_from_db(db_connection, table_name, db_type, schema_name=None):
    try:
        if not schema_name:
            logging.error(f"Schema name is required for {db_type}.")
            return {}

        if db_type == "Oracle":
            query = f"""
                SELECT lower(COLUMN_NAME) AS COLUMN_NAME, lower(DATA_TYPE) AS DATA_TYPE
                FROM ALL_TAB_COLUMNS 
                WHERE upper(TABLE_NAME) = '{table_name.upper()}'
                AND upper(OWNER) = '{schema_name.upper()}'
            """
        
        elif db_type in ["MySql", "MariaDB", "Postgresql", "MSSQL"]:
            query = f"""
                SELECT lower(COLUMN_NAME) AS COLUMN_NAME, lower(DATA_TYPE) AS DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE lower(TABLE_SCHEMA) = '{schema_name.lower()}' 
                AND lower(TABLE_NAME) = '{table_name.lower()}'
            """
        
        columns_df = pd.read_sql(query, con=db_connection)
        
        columns_df.columns = columns_df.columns.str.upper()
        columns_dict = {row['COLUMN_NAME']: row['DATA_TYPE'] for _, row in columns_df.iterrows()}
        return columns_dict
    
    except Exception as e:
        logging.error(f"Error fetching columns and types for {db_type} (Schema: {schema_name}): {e}")
        return {}

def check_data_type_mismatch(source_df, destination_column_types):
    mismatch_found = False
    for column in source_df.columns:
        source_dtype = source_df[column].dtype
        dest_dtype = destination_column_types.get(column.upper())
        
        if dest_dtype:
            source_dtype_str = str(source_dtype)
            if "datetime" in source_dtype_str.lower() and "DATE" not in dest_dtype:
                logging.error(f"Data type mismatch for column '{column}': Source type '{source_dtype}' does not match DB type '{dest_dtype}'")
                mismatch_found = True
            elif "int" in source_dtype_str.lower() and "INT" not in dest_dtype:
                logging.error(f"Data type mismatch for column '{column}': Source type '{source_dtype}' does not match DB type '{dest_dtype}'")
                mismatch_found = True
            elif "float" in source_dtype_str.lower() and "FLOAT" not in dest_dtype:
                logging.error(f"Data type mismatch for column '{column}': Source type '{source_dtype}' does not match DB type '{dest_dtype}'")
                mismatch_found = True
            elif "object" in source_dtype_str.lower() and "VARCHAR" not in dest_dtype:
                logging.error(f"Data type mismatch for column '{column}': Source type '{source_dtype}' does not match DB type '{dest_dtype}'")
                mismatch_found = True

    return mismatch_found

def extract_data_from_excel(excel_file_path, sheet_name=None):
    try:
        if os.path.exists(excel_file_path):
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name) if sheet_name else pd.read_excel(excel_file_path)
            
            if df.empty:
                logging.warning(f"Excel file '{excel_file_path}' is empty.")
            else:
                logging.info(f"Extracted {len(df)} rows and {len(df.columns)} columns from the Excel file.")
            return df
        else:
            logging.error(f"Excel file not found: {excel_file_path}")
            return None
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}")
        return None

def extract_data_from_db(query, db_connection):
    try:
        df = pd.read_sql(query, con=db_connection)
        
        if df.empty:
            logging.warning(f"No data returned for query: {query}")
        else:
            logging.info(f"Extracted {len(df)} rows and {len(df.columns)} columns from the database.")
        
        return df
    except Exception as e:
        logging.error(f"Error extracting data from DB: {e}")
        return None

def transform_data(df, source_columns, destination_columns):
    try:
        logging.info(f"Initial DataFrame columns: {df.columns.tolist()}")
        logging.info(f"Initial DataFrame shape: {df.shape}")

        df.columns = df.columns.str.lower()  
        matching_columns = [col for col in df.columns if col in destination_columns]
        
        extra_source_columns = set(source_columns) - set(df.columns)
        if extra_source_columns:
            logging.warning(f"The following source columns are missing in the data: {extra_source_columns}")

        missing_columns = set(df.columns) - set(destination_columns)
        if missing_columns:
            logging.warning(f"The following columns are in the source but not in the destination: {missing_columns}")

        df_transformed = df[matching_columns] 

        logging.info(f"Transformed DataFrame columns: {df_transformed.columns.tolist()}")
        logging.info(f"Transformed DataFrame shape: {df_transformed.shape}")
        
        return df_transformed
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        return df

def load_data_to_db(df, table_name, db_connection):
    try:
        logging.info(f"Attempting to load {len(df)} rows into {table_name}.")
        df.to_sql(table_name, db_connection, if_exists='replace', index=False)
        logging.info(f"Data loaded into {table_name} table successfully.")
    except Exception as e:
        logging.error(f"Error loading data to {table_name}: {e}")

def etl_process(etl_pr_dml: str, from_db: str, to_db: str, query: str, table_name: str, excel_file_path: str = None, source_schema: str = None, target_schema: str = None):
    try:
        start_time = datetime.now()
        process_name = f"ETL_{from_db}_to_{to_db}_{table_name}"
        
        log_etl_db_config = get_connection(etl_pr_dml, 1)  
        log_etl_process(log_etl_db_config, process_name, 'STARTED', 'ETL process initiated', start_time=start_time)

        from_connection = get_connection(from_db)
        to_connection = get_connection(to_db)

        if not from_connection or not to_connection:
            log_etl_process(log_etl_db_config, process_name, 'FAILED', 'Connection error', start_time=start_time)
            return
        
        logging.info(f"Connected to {from_db} and {to_db} successfully.")
        
        source_column_types = get_columns_and_types_from_db(from_connection, table_name, from_db, source_schema)
        destination_column_types = get_columns_and_types_from_db(to_connection, table_name, to_db, target_schema)

        if from_db == "Excel" and excel_file_path:
            df = extract_data_from_excel(excel_file_path)
        elif from_db in ["Oracle", "Postgresql", "MySql", "MariaDB", "MSSQL"] and query:
            df = extract_data_from_db(query, from_connection)
        else:
            log_etl_process(log_etl_db_config, process_name, 'FAILED', 'Invalid input for data extraction', start_time=start_time)
            return

        if df is None or df.empty:
            log_etl_process(log_etl_db_config, process_name, 'FAILED', 'No data extracted', start_time=start_time)
            return
        
        logging.info(f"Extracted {len(df)} rows of data.")

        source_columns = set(source_column_types.keys()) if from_db != "Excel" else set(df.columns.str.upper())
        mismatch = check_data_type_mismatch(df, destination_column_types)
        if mismatch:
            log_etl_process(log_etl_db_config, process_name, 'FAILED', 'Data type mismatch detected', start_time=start_time)
            return
                   
        df_transformed = transform_data(df, source_columns, destination_column_types.keys())
        load_data_to_db(df_transformed, table_name, to_connection)

        if hasattr(from_connection, 'dispose'):
            from_connection.dispose()
        if hasattr(to_connection, 'dispose'):
            to_connection.dispose()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        log_etl_process(log_etl_db_config, process_name, 'COMPLETED', f'ETL process completed successfully. Duration: {duration:.2f} seconds', start_time=start_time, end_time=end_time)
        logging.info(f"ETL process completed in {duration:.2f} seconds.")

    except Exception as e:
        end_time = datetime.now()
        log_etl_process(log_etl_db_config, process_name, 'FAILED', str(e), start_time=start_time, end_time=end_time)
        logging.error(f"ETL process failed: {e}")

if __name__ == '__main__':
    etl_pr_dml,from_db,to_db,query,table_name,excel_file_path,source_schema,target_schema = "Oracle","Oracle","Postgresql","SELECT * FROM EMP_TEST","emp_test",None,"HR","public"
    etl_process(etl_pr_dml, from_db, to_db, query, table_name, excel_file_path, source_schema, target_schema)
