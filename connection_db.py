import cx_Oracle
import pyodbc
from sqlalchemy import create_engine
from psycopg2 import OperationalError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class db_connection:

    #--------------------------------------------------------------------------------------------------------------------------------------------------- Oracle connection
    @staticmethod
    def connect_oracle(ints: int, host: str = 'localhost', port: int = 1521, service_name: str = 'xepdb1', user: str = 'hr', password: str = 'hr1'):
        try:
            if ints == 1:
                cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\dbhomeXE\bin")
            
            dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
            oracle_url = f"oracle+cx_oracle://{user}:{password}@{dsn_tns}"
            engine = create_engine(oracle_url)
            logging.info(f"Successfully connected to Oracle Database at {host}:{port} via SQLAlchemy")
            return engine
        except cx_Oracle.DatabaseError as e:
            logging.error(f'Oracle Database Connection Failed: {e}')
            return None

    #--------------------------------------------------------------------------------------------------------------------------------------------------- PostgreSQL connection
    @staticmethod
    def connect_postgresql(host: str = 'localhost', port: int = 5432, db: str = 'postgres', user: str = 'postgres', password: str = 'hr1'):
        try:
            postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
            engine = create_engine(postgres_url)
            logging.info(f"Successfully connected to PostgreSQL Database at {host}:{port} via SQLAlchemy")
            return engine
        except OperationalError as e:
            logging.error(f'PostgreSQL Database Connection Failed: {e}')
            return None

    #--------------------------------------------------------------------------------------------------------------------------------------------------- MySQL connection
    @staticmethod
    def connect_mysql(host: str = 'localhost', port: int = 3306, db: str = 'mysql_db', user: str = 'mysql_user', password: str = 'mysql_password'):
        try:
            mysql_url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
            engine = create_engine(mysql_url)
            logging.info(f"Successfully connected to MySQL Database at {host}:{port} via SQLAlchemy")
            return engine
        except Exception as e:
            logging.error(f'MySQL Database Connection Failed: {e}')
            return None

    #--------------------------------------------------------------------------------------------------------------------------------------------------- MariaDB connection
    @staticmethod
    def connect_mariadb(host: str = 'localhost', port: int = 3306, db: str = 'mariadb_db', user: str = 'mariadb_user', password: str = 'mariadb_password'):
        try:
            mariadb_url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
            engine = create_engine(mariadb_url)
            logging.info(f"Successfully connected to MariaDB Database at {host}:{port} via SQLAlchemy")
            return engine
        except Exception as e:
            logging.error(f'MariaDB Database Connection Failed: {e}')
            return None

    #--------------------------------------------------------------------------------------------------------------------------------------------------- MSSQL connection
    @staticmethod
    def connect_mssql(host: str = 'localhost', port: int = 1433, db: str = 'mssql_db', user: str = 'mssql_user', password: str = 'mssql_password', driver: str = '{ODBC Driver 17 for SQL Server}'):
        try:
            mssql_url = f'mssql+pyodbc://{user}:{password}@{host}:{port}/{db}?driver={driver}'
            engine = create_engine(mssql_url)
            logging.info(f"Successfully connected to MSSQL Database at {host}:{port} via SQLAlchemy")
            return engine
        except pyodbc.Error as e:
            logging.error(f'MSSQL Database Connection Failed: {e}')
            return None
