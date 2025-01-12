import pandas as pd
import os
import connection_db as c_db
import logging
from sqlalchemy import text, inspect
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class db_process:

    @staticmethod
    def from_get_connection(db_type: str, ints: int = None,fm_db: dict=None):
        try:
            if db_type == "Oracle":
                return c_db.db_connection.connect_oracle(ints=ints,host=fm_db['host'],port=fm_db['port'],service_name=fm_db['service_name'],user=fm_db['user'],password=fm_db['password'])
            elif db_type == "Postgresql":
                return c_db.db_connection.connect_postgresql(host=fm_db['host'],port=fm_db['port'],db=fm_db['service_name'],user=fm_db['user'],password=fm_db['password'])
            elif db_type == "MySql":
                return c_db.db_connection.connect_mysql(host=fm_db['host'],port=fm_db['port'],db=fm_db['service_name'],user=fm_db['user'],password=fm_db['password'])
            elif db_type == "MariaDB":
                return c_db.db_connection.connect_mariadb(host=fm_db['host'],port=fm_db['port'],db=fm_db['service_name'],user=fm_db['user'],password=fm_db['password'])
            elif db_type == "MSSQL":
                return c_db.db_connection.connect_mssql(host=fm_db['host'],port=fm_db['port'],db=fm_db['service_name'],user=fm_db['user'],password=fm_db['password'],driver=None)
            else:
                logging.error(f"Unsupported database type: {db_type}")
                return None
        except Exception as e:
            logging.error(f"Error connecting to {db_type}: {e}")
            return None
    
    @staticmethod
    def to_get_connection(db_type: str,ints: int = None,to_db: dict = None):
        try:
            if db_type == "Oracle":
                return c_db.db_connection.connect_oracle(ints=ints,host=to_db['host'],port=to_db['port'],service_name=to_db['service_name'],user=to_db['user'],password=to_db['password'])
            elif db_type == "Postgresql":
                return c_db.db_connection.connect_postgresql(host=to_db['host'],port=to_db['port'],db=to_db['service_name'],user=to_db['user'],password=to_db['password'])
            elif db_type == "MySql":
                return c_db.db_connection.connect_mysql(host=to_db['host'],port=to_db['port'],db=to_db['service_name'],user=to_db['user'],password=to_db['password'])
            elif db_type == "MariaDB":
                return c_db.db_connection.connect_mariadb(host=to_db['host'],port=to_db['port'],db=to_db['service_name'],user=to_db['user'],password=to_db['password'])
            elif db_type == "MSSQL":
                return c_db.db_connection.connect_mssql(host=to_db['host'],port=to_db['port'],db=to_db['service_name'],user=to_db['user'],password=to_db['password'],driver=None)
            else:
                logging.error(f"Unsupported database type: {db_type}")
                return None
        except Exception as e:
            logging.error(f"Error connecting to {db_type}: {e}")
            return None
        
    @staticmethod
    def log_etl_process(engine, process_name, status, details, start_time=None, end_time=None, from_table: str = None, to_table: str = None):
        try:
            query = text("""
                MERGE INTO etl_process_log target
                USING (SELECT :process_name AS process_name, :status AS status, :details AS details, 
                    TO_TIMESTAMP(TO_CHAR(:start_time,'RRRR-MM-DD HH24:MI:SS'), 'RRRR-MM-DD HH24:MI:SS') AS start_time,
                    TO_TIMESTAMP(TO_CHAR(:end_time,'RRRR-MM-DD HH24:MI:SS'), 'RRRR-MM-DD HH24:MI:SS') AS end_time,
                    :from_table as from_table,:to_table as to_table FROM dual) source
                ON (target.process_name = source.process_name AND target.from_table = source.from_table
                    AND target.to_table = source.to_table 
                    )
                WHEN MATCHED THEN
                    UPDATE SET status = source.status, details = source.details, end_time = source.end_time
                WHEN NOT MATCHED THEN
                    INSERT (process_name, status, details, start_time, end_time, from_table, to_table)
                    VALUES (source.process_name, source.status, source.details, source.start_time, source.end_time, source.from_table, source.to_table)
            """)

            with engine.connect() as connection:
                result = connection.execute(query, {
                    'process_name': process_name,
                    'status': status,
                    'details': details,
                    'start_time': start_time,
                    'end_time': end_time,
                    'from_table': from_table,
                    'to_table': to_table
                })
                connection.commit()
                logging.info(f"Rows affected: {result.rowcount}")

            logging.info(f"ETL process logged successfully: {process_name}, Status: {status}")

        except Exception as e:
            logging.error(f"Failed to log ETL process: {e}")

    @staticmethod
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
                    FROM information_schema.COLUMNS 
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

    @staticmethod
    def check_data_type_mismatch(source_df, destination_column_types):
        mismatch_found = False
        type_mappings = {
                            "datetime": ["DATE", "TIMESTAMP", "DATETIME", "TIME", "YEAR", "DATETIME2", "SMALLDATETIME"],
                            "int": ["INT", "INTEGER", "NUMBER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT", "SERIAL", "BIGSERIAL"],
                            "float": ["FLOAT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "MONEY", "SMALLMONEY"],
                            "object": ["VARCHAR2", "VARCHAR", "CHAR", "TEXT", "CLOB", "NVARCHAR", "LONGTEXT", "NCHAR"],
                            "bytes": ["RAW", "BLOB", "CLOB", "VARBINARY", "BINARY", "LONGBLOB", "IMAGE", "TEXT"],
                            "boolean": ["BOOLEAN", "BOOL", "BIT", "TINYINT(1)", "CHAR(1)"],
                            "date": ["DATE", "DATETIME", "TIMESTAMP", "DATEONLY", "DATE", "TIME", "YEAR", "DATETIME2"],
                            "json": ["JSON", "JSONB", "TEXT", "LONGTEXT", "XML"],
                            "uuid": ["UUID", "GUID", "CHAR(36)", "VARCHAR(36)", "UNIQUEIDENTIFIER"],
                            "enum": ["ENUM", "SET"],
                            "spatial": ["POINT", "LINESTRING", "POLYGON", "GEOMETRY", "GEOGRAPHY", "GEOMETRYCOLLECTION", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"],
                            "interval": ["INTERVAL", "TIME", "TIMEZONE", "DATE", "DATETIME"],
                            "currency": ["MONEY", "SMALLMONEY", "DECIMAL(19,4)"]
                        }
        
        for column in source_df.columns:
            source_dtype = source_df[column].dtype
            dest_dtype = destination_column_types.get(column.upper())
            if dest_dtype:
                source_dtype_str = str(source_dtype).lower()
                dest_dtype_upper = dest_dtype.upper()
                type_matched = False
                for source_type, valid_dest_types in type_mappings.items():
                    if source_type in source_dtype_str:
                        if any(valid_dest_type in dest_dtype_upper for valid_dest_type in valid_dest_types):
                            type_matched = True
                        break

                if not type_matched:
                    logging.error(
                        f"Data type mismatch for column '{column}': Source type '{source_dtype}' does not match DB type '{dest_dtype}'"
                    )
                    mismatch_found = True

        return mismatch_found

    @staticmethod
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

    @staticmethod
    def transform_data(df, source_columns, destination_columns, handle_null='drop'):
        try:
            logging.info(f"Initial DataFrame columns: {df.columns.tolist()}")
            logging.info(f"Initial DataFrame shape: {df.shape}")

            df.columns = df.columns.astype(str).str.lower()
            df = df.drop_duplicates()

            if handle_null == 'drop':
                df = df.dropna()
            elif handle_null == 'fill':
                df = df.fillna(0)

            matching_columns = [col for col in destination_columns if col in df.columns]

            extra_source_columns = set(source_columns) - set(df.columns)
            if extra_source_columns:
                logging.warning(f"The following source columns are missing in the data: {extra_source_columns}")

            missing_columns = set(df.columns) - set(destination_columns)
            if missing_columns:
                logging.warning(f"The following columns are in the source but not in the destination: {missing_columns}")

            df_transformed = df[matching_columns]
            logging.info(f"Transformed DataFrame shape: {df_transformed.shape}")

            return df_transformed
        except Exception as e:
            logging.error(f"Error in transforming data: {e}")
            return None

    @staticmethod
    def load_data_to_db(df, table_name, db_connection):
        try:
            logging.info(f"Attempting to load {len(df)} rows into the table {table_name},{db_connection}")
            df.to_sql(table_name, con=db_connection, if_exists='append', index=False)
            logging.info(f"Loaded {len(df)} rows into the table {table_name}")
        except Exception as e:
            logging.error(f"Error loading data into {table_name}: {e}")

    @staticmethod
    def etl_process(etl_pr_dml: str, from_db: str, to_db: str, query: str, table_name: str, target_schema: str = None,from_db_config: dict = None, to_db_config: dict = None, etl_pr_dml_db_config: dict = None):
        try:
            start_time = datetime.now()

            if query:
                match = re.search(r"from\s+(\w+)", query, re.IGNORECASE)
                process_name = f"ETL_{from_db}_{match.group(1)}_to_{to_db}_{table_name}" if match else "ETL_PROCESS"
            else:
                process_name = f"ETL_{from_db}_to_{to_db}_{table_name}"

            global log_etl_db_config
            log_etl_db_config = db_process.from_get_connection(db_type=etl_pr_dml, ints=1, fm_db=etl_pr_dml_db_config)
            db_process.log_etl_process(log_etl_db_config, process_name, 'STARTED', 'ETL process initiated',
                                    start_time=start_time, from_table=query, to_table=table_name)

            from_connection = db_process.from_get_connection(db_type=from_db, fm_db=from_db_config)
            to_connection = db_process.to_get_connection(db_type=to_db, to_db=to_db_config)

            if not to_connection or (from_connection is None and from_db != "Excel"):
                db_process.log_etl_process(log_etl_db_config, process_name, 'FAILED', 'Connection error',
                                        start_time=start_time, from_table=query, to_table=table_name)
                return

            logging.info(f"Connected to {from_db} and {to_db}")

            if from_connection:
                source_data = db_process.extract_data_from_db(query, from_connection)

            if source_data is None or source_data.empty:
                db_process.log_etl_process(log_etl_db_config, process_name, 'FAILED',
                                        'Source data extraction failed or is empty', start_time=start_time,
                                        from_table=query, to_table=table_name)
                return

            destination_columns = db_process.get_columns_and_types_from_db(to_connection, table_name, to_db, target_schema)

            if db_process.check_data_type_mismatch(source_data, destination_columns):
                db_process.log_etl_process(log_etl_db_config, process_name, 'FAILED', 'Data type mismatch',
                                        start_time=start_time, from_table=query, to_table=table_name)
                return

            transformed_data = db_process.transform_data(source_data, source_data.columns.tolist(), destination_columns.keys())

            db_process.load_data_to_db(transformed_data, table_name, to_connection)

            end_time = datetime.now()
            db_process.log_etl_process(log_etl_db_config, process_name, 'COMPLETED', 'ETL process completed successfully',
                                    start_time=start_time, end_time=end_time, from_table=query, to_table=table_name)

        except Exception as e:
            logging.error(f"ETL process failed: {e}")
            end_time = datetime.now()
            db_process.log_etl_process(log_etl_db_config, process_name, 'FAILED', f"ETL process failed: {e}",
                                    start_time=start_time, end_time=end_time, from_table=query, to_table=table_name)
            
class ExcelProcess:
    @staticmethod
    def detect_file_type(file_path):
        if file_path.lower().endswith('.xlsx'):
            return 'excel'
        elif file_path.lower().endswith('.csv'):
            return 'csv'
        else:
            logging.error(f"Unsupported file format for file: {file_path}")
            return None
    
    @staticmethod
    def extract_data_from_excel(excel_file_path, sheet_name=None):
        if not os.path.exists(excel_file_path):
            logging.error(f"Excel file not found: {excel_file_path}")
            return None
        
        try:
            if not sheet_name:
                sheet_names = pd.ExcelFile(excel_file_path).sheet_names
                if len(sheet_names) > 1:
                    logging.warning(f"Multiple sheets found: {sheet_names}. Defaulting to the first sheet.")
                sheet_name = sheet_names[0]
            
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
            
            if df.empty:
                logging.warning(f"Excel file '{excel_file_path}' is empty or contains no data.")
            else:
                logging.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns from sheet '{sheet_name}'.")

            df.columns = df.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
            logging.info(f"Columns after cleaning: {df.columns}")
            
            return df

        except ValueError as e:
            logging.error(f"Error reading Excel file: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return None

    @staticmethod
    def create_table_from_df(df, engine, table_name):
        try:
            if not inspect(engine).has_table(table_name):
                logging.info(f"Table '{table_name}' does not exist. Creating table...")

                df.head(0).to_sql(table_name, con=engine, if_exists='fail', index=False)
                logging.info(f"Table '{table_name}' created successfully.")
            else:
                logging.info(f"Table '{table_name}' already exists.")
        except Exception as e:
            logging.error(f"Error creating table '{table_name}': {e}")

    @staticmethod
    def insert_data_to_db(df, engine, table_name):
        try:
            df.to_sql(name=table_name, con=engine, index=False, if_exists='append', chunksize=500)
            logging.info(f"Data successfully inserted into '{table_name}' table.")
        except Exception as e:
            logging.error(f"Error inserting data into '{table_name}' table: {e}")

    @staticmethod
    def etl_process_excel(file_path: str, to_db_nm: str, table_name: str, to_db_config: dict = None, etl_pr_dml_db_config: dict = None):
        etl_db_cre = None  
        process_name = None 
        try:
            file_type = ExcelProcess.detect_file_type(file_path)
            if file_type is None:
                return
            if file_type == 'excel':
                df = ExcelProcess.extract_data_from_excel(file_path)
            elif file_type == 'csv':
                df = pd.read_csv(file_path)
                df.columns = df.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
                logging.info(f"CSV file loaded with {len(df)} rows and {len(df.columns)} columns.")
            else:
                logging.error(f"Unsupported file type: {file_type}.")
                return

            if df is None or df.empty:
                logging.error(f"ETL process failed: No data extracted from {file_path}.")
                return
            
            if to_db_nm == "Oracle":
                con_db = db_process.from_get_connection(db_type=to_db_nm, ints=1, fm_db=to_db_config)
            else:
                con_db = db_process.to_get_connection(db_type=to_db_nm, to_db=to_db_config)

            if not con_db:
                logging.error(f"ETL process failed: Unable to connect to the target database {to_db_nm}.")
                return

            process_name = f"ETL_{file_type.capitalize()}_{table_name}_to_{to_db_nm}"

            start_time = datetime.now()

            if to_db_nm == "Oracle":
                etl_db_cre = db_process.from_get_connection(db_type="Oracle", ints=None, fm_db=etl_pr_dml_db_config)
            else:
                etl_db_cre = db_process.from_get_connection(db_type="Oracle", ints=1, fm_db=etl_pr_dml_db_config)

            db_process.log_etl_process(etl_db_cre, process_name, 'STARTED', 'ETL process initiated', start_time=start_time, from_table=file_path, to_table=table_name)

            with con_db.begin():
                ExcelProcess.create_table_from_df(df, con_db, table_name)
                ExcelProcess.insert_data_to_db(df, con_db, table_name)

            end_time = datetime.now()
            db_process.log_etl_process(etl_db_cre, process_name, 'COMPLETED', 'ETL process completed successfully', start_time=start_time, end_time=end_time, from_table=file_path, to_table=table_name)

        except Exception as e:
            logging.error(f"ETL process failed: {e}")
            end_time = datetime.now()
            if etl_db_cre is None:
                logging.error(f"ETL process failed and database connection could not be established.")
            else:
                db_process.log_etl_process(etl_db_cre, process_name, 'FAILED', f"ETL process failed: {e}", start_time=start_time, end_time=end_time, from_table=file_path, to_table=table_name)



etl_process_val=input('Enter The Process:')
from_db=input('Enter The DB Name:')
to_table_name=input('Enter the Table Name:')

etl_pr_dml="Oracle"
etl_pr_dml_db_config={
                        "host" : "localhost",
                        "port" : 1521,
                        "service_name" : "xepdb1",
                        "user" : "hr",
                        "password" : "hr1"
                    }

if __name__ == '__main__' and etl_process_val == 'db':
    from_db_config={
                    "host" : "localhost",
                    "port" : 1521,
                    "service_name" : "xepdb1",
                    "user" : "hr",
                    "password" : "hr1"
                }
    to_db_config={
                    "host" : "localhost",
                    "port" : 5432,
                    "service_name" : "postgres",
                    "user" : "postgres",
                    "password" : "hr1"
                }
    
    from_db, to_db, query, table_name,target_schema ="Oracle", "Postgresql", "SELECT * FROM ETL_PROCESS_DATA", "etl_process_data", "public"
    db_process.etl_process(etl_pr_dml, from_db, to_db, query, table_name,target_schema,from_db_config,to_db_config,etl_pr_dml_db_config)
else:
    forexcel_to_db_config={
                            "host" : "localhost",
                            "port" : 1521,
                            "service_name" : "xepdb1",
                            "user" : "hr",
                            "password" : "hr1"
                        }
    ExcelProcess.etl_process_excel(r"d:\BH Store List.xlsx",from_db,to_table_name,forexcel_to_db_config,etl_pr_dml_db_config)
