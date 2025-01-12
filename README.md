# Database Connection Guide Using Python and SQLAlchemy

## Introduction

This guide demonstrates how to use Python and the SQLAlchemy library to connect to multiple types of databases, including **Oracle**, **PostgreSQL**, **MySQL**, **MariaDB**, and **MSSQL**. SQLAlchemy is a powerful toolkit and Object Relational Mapper (ORM) that provides a consistent interface for connecting and interacting with different types of databases. This guide will walk you through setting up the environment, understanding the code, and how to establish connections with various databases.

By using this library, developers can work with multiple database systems while keeping the connection logic consistent across them.

## Features

- **Oracle**: Supports connecting to Oracle databases via the `cx_Oracle` library.
- **PostgreSQL**: Supports connecting to PostgreSQL databases via the `psycopg2` library.
- **MySQL**: Supports connecting to MySQL databases via the `pymysql` library.
- **MariaDB**: Supports connecting to MariaDB databases (which is compatible with MySQL) via the `pymysql` library.
- **MSSQL**: Supports connecting to MSSQL databases via the `pyodbc` library.

## Installation

Before using this guide, ensure you have the necessary libraries installed. You can install the required packages using `pip`:

```bash
pip install sqlalchemy cx_Oracle pyodbc psycopg2 pymysql
