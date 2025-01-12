Database Connection Library
This project provides a Python library that facilitates connections to various relational databases using SQLAlchemy. The library supports connecting to multiple database types, including Oracle, PostgreSQL, MySQL, MariaDB, and MSSQL.
Features
- **Oracle**: Supports connecting to Oracle databases via `cx_Oracle`.
- **PostgreSQL**: Supports connecting to PostgreSQL databases via SQLAlchemy with the `psycopg2` driver.
- **MySQL**: Supports connecting to MySQL databases via SQLAlchemy with the `pymysql` driver.
- **MariaDB**: Supports connecting to MariaDB databases (since MariaDB is compatible with MySQL) via `pymysql`.
- **MSSQL**: Supports connecting to MSSQL databases via SQLAlchemy with the `pyodbc` driver.
Installation
To install the required libraries, use `pip` to install the dependencies:

```bash
pip install cx_Oracle sqlalchemy psycopg2 pymysql pyodbc
```
Usage
1. Oracle Database Connection
The following method allows you to connect to an Oracle database using the `cx_Oracle` driver. You need to specify the host, port, service name, user, and password for the connection.
```python
from your_module import db_connection

engine = db_connection.connect_oracle(
    ints=1, 
    host='localhost', 
    port=1521, 
    service_name='xepdb1', 
    user='hr', 
    password='hr1'
)
```
- `ints`: If set to `1`, the Oracle client is initialized using the `cx_Oracle.init_oracle_client()` method.
- `host`: The host where the Oracle database is running (default: `localhost`).
- `port`: The port the Oracle database is listening on (default: `1521`).
- `service_name`: The service name of the Oracle database (default: `xepdb1`).
- `user`: The username for connecting to the Oracle database.
- `password`: The password for the provided username.
2. PostgreSQL Database Connection
The following method allows you to connect to a PostgreSQL database using the `psycopg2` driver.
```python
engine = db_connection.connect_postgresql(
    host='localhost', 
    port=5432, 
    db='postgres', 
    user='postgres', 
    password='hr1'
)
```
- `host`: The host where the PostgreSQL database is running (default: `localhost`).
- `port`: The port the PostgreSQL database is listening on (default: `5432`).
- `db`: The name of the database you want to connect to (default: `postgres`).
- `user`: The username for connecting to the PostgreSQL database.
- `password`: The password for the provided username.
3. MySQL Database Connection
The following method allows you to connect to a MySQL database using the `pymysql` driver.
```python
engine = db_connection.connect_mysql(
    host='localhost', 
    port=3306, 
    db='mysql_db', 
    user='mysql_user', 
    password='mysql_password'
)
```
- `host`: The host where the MySQL database is running (default: `localhost`).
- `port`: The port the MySQL database is listening on (default: `3306`).
- `db`: The name of the MySQL database (default: `mysql_db`).
- `user`: The username for connecting to the MySQL database.
- `password`: The password for the provided username.
4. MariaDB Database Connection
The following method allows you to connect to a MariaDB database (which is compatible with MySQL) using the `pymysql` driver.
```python
engine = db_connection.connect_mariadb(
    host='localhost', 
    port=3306, 
    db='mariadb_db', 
    user='mariadb_user', 
    password='mariadb_password'
)
```
- `host`: The host where the MariaDB database is running (default: `localhost`).
- `port`: The port the MariaDB database is listening on (default: `3306`).
- `db`: The name of the MariaDB database (default: `mariadb_db`).
- `user`: The username for connecting to the MariaDB database.
- `password`: The password for the provided username.
5. MSSQL Database Connection
The following method allows you to connect to an MSSQL database using the `pyodbc` driver.
```python
engine = db_connection.connect_mssql(
    host='localhost', 
    port=1433, 
    db='mssql_db', 
    user='mssql_user', 
    password='mssql_password', 
    driver='{ODBC Driver 17 for SQL Server}'
)
```
- `host`: The host where the MSSQL database is running (default: `localhost`).
- `port`: The port the MSSQL database is listening on (default: `1433`).
- `db`: The name of the MSSQL database (default: `mssql_db`).
- `user`: The username for connecting to the MSSQL database.
- `password`: The password for the provided username.
- `driver`: The ODBC driver name to use (default: `{ODBC Driver 17 for SQL Server}`).
Logging
This library integrates Python's `logging` module to keep track of successful connections and errors. Log messages are generated to inform users of connection statuses or issues during database connection attempts.
### Example log output:
```
2025-01-12 12:34:56,789 - INFO - Successfully connected to Oracle Database at localhost:1521 via SQLAlchemy
```
In case of failure, the following log message may appear:
```
2025-01-12 12:34:56,789 - ERROR - Oracle Database Connection Failed: [Error details]
```
Exception Handling
If the connection to any database fails, the function returns `None`, and an error message is logged. You can handle this in your application with proper exception handling, ensuring that your program continues to run smoothly.
Contribution
Contributions to this project are welcome! If you'd like to improve the functionality, add more database support, or suggest enhancements, feel free to fork the repository and submit a pull request.
