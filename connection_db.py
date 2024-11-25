import cx_Oracle
import os
import psycopg2
import pyodbc
from sqlalchemy import create_engine
from psycopg2 import OperationalError
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class db_connection:
    
    @staticmethod
    def connect_oracle(ints: int):
        try:
            if ints == 1 :
                cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\dbhomeXE\bin")
            oracle_user = os.getenv('ORACLE_USER', 'hr')
            oracle_password = os.getenv('ORACLE_PASSWORD', 'hr1')
            dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xepdb1')

            oracle_url = f"oracle+cx_oracle://{oracle_user}:{oracle_password}@{dsn_tns}"
            engine = create_engine(oracle_url)
            logging.info("Successfully connected to Oracle Database via SQLAlchemy")
            return engine
        except cx_Oracle.DatabaseError as e:
            logging.error(f'Oracle Database Connection Failed: {e}')
            return None

    @staticmethod
    def connect_postgresql():
        try:
            postgres_user = os.getenv('POSTGRES_USER', 'postgres')
            postgres_password = os.getenv('POSTGRES_PASSWORD', 'hr1')
            postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
            postgres_port = os.getenv('POSTGRES_PORT', '5432')
            postgres_db = os.getenv('POSTGRES_DB', 'demo_db')
            postgres_url = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'
            engine = create_engine(postgres_url)
            logging.info("Successfully connected to PostgreSQL Database via SQLAlchemy")
            return engine
        except OperationalError as e:
            logging.error(f'PostgreSQL Database Connection Failed: {e}')
            return None

    @staticmethod
    def connect_mysql():
        try:
            mysql_user = os.getenv('MYSQL_USER', 'mysql_user')
            mysql_password = os.getenv('MYSQL_PASSWORD', 'mysql_password')
            mysql_host = os.getenv('MYSQL_HOST', 'localhost')
            mysql_port = os.getenv('MYSQL_PORT', '3306')
            mysql_db = os.getenv('MYSQL_DB', 'mysql_db')
            mysql_url = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}'
            engine = create_engine(mysql_url)
            logging.info("Successfully connected to MySQL Database via SQLAlchemy")
            return engine
        except Exception as e:
            logging.error(f'MySQL Database Connection Failed: {e}')
            return None

    @staticmethod
    def connect_mariadb():
        try:
            mariadb_user = os.getenv('MARIADB_USER', 'mariadb_user')
            mariadb_password = os.getenv('MARIADB_PASSWORD', 'mariadb_password')
            mariadb_host = os.getenv('MARIADB_HOST', 'localhost')
            mariadb_port = os.getenv('MARIADB_PORT', '3306')
            mariadb_db = os.getenv('MARIADB_DB', 'mariadb_db')
            mariadb_url = f'mysql+pymysql://{mariadb_user}:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_db}'
            engine = create_engine(mariadb_url)
            logging.info("Successfully connected to MariaDB Database via SQLAlchemy")
            return engine
        except Exception as e:
            logging.error(f'MariaDB Database Connection Failed: {e}')
            return None

    @staticmethod
    def connect_mssql():
        try:
            mssql_driver = os.getenv('MSSQL_DRIVER', '{ODBC Driver 17 for SQL Server}')
            mssql_user = os.getenv('MSSQL_USER', 'mssql_user')
            mssql_password = os.getenv('MSSQL_PASSWORD', 'mssql_password')
            mssql_host = os.getenv('MSSQL_HOST', 'localhost')
            mssql_port = os.getenv('MSSQL_PORT', '1433')
            mssql_db = os.getenv('MSSQL_DB', 'mssql_db')
            mssql_url = f'mssql+pyodbc://{mssql_user}:{mssql_password}@{mssql_host}:{mssql_port}/{mssql_db}?driver={mssql_driver}'
            engine = create_engine(mssql_url)
            logging.info("Successfully connected to MSSQL Database via SQLAlchemy")
            return engine
        except pyodbc.Error as e:
            logging.error(f'MSSQL Database Connection Failed: {e}')
            return None
