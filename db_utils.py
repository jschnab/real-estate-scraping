import logging
import os

from configparser import ConfigParser
from pathlib import Path

import psycopg2
import psycopg2.extras

from psycopg2 import sql

from sql_commands import (
    COPY_FROM_NO_HEADER_SQL,
    COPY_FROM_WITH_HEADER_SQL,
    COPY_TO_NO_HEADER_SQL,
    COPY_TO_WITH_HEADER_SQL,
    TABLE_EXISTS_SQL,
)

HOME = str(Path.home())
CONFIG_FILE = os.path.join(HOME, ".browsing", "browser.conf")

config = ConfigParser()
config.read(CONFIG_FILE)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    filename=os.path.join(HOME, ".browsing", "browser.log"),
    filemode="a")


def get_connection(autocommit=False):
    """
    Get a connection to a PostgreSQL database.

    :param bool autocommit: if SQL commands should be automatically committed
                            (optional, default False)
    :return: connection object
    """
    config = ConfigParser()
    config.read(CONFIG_FILE)
    database = config["database"]["database_name"]
    username = config["database"]["username"]
    password = config["database"]["password"]
    host = config["database"]["host"]
    port = int(config["database"]["port"])
    con = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=host,
        port=port,
    )
    con.autocommit = autocommit
    return con


def execute_sql(statement, parameters=None, connection=None):
    """
    Execute a SQL statement.

    Statements should be passed this way:
    [("SELECT * FROM friends WHERE age > %s", (42,)), ...]

    :param str statement: SQL statement
    :param tuple parameters: parameters of the SQL statement (optional)
    :param connection: a psycopg2 connection object (optional)
    """
    if not connection:
        connection = get_connection(autocommit=True)
    cur = connection.cursor()
    try:
        cur.execute(statement, parameters)
    except Exception as e:
        logging.critical(e)
        connection.rollback()
        raise
    finally:
        connection.close()


def query_sql(statement, parameters=None, connection=None):
    """
    Query the database.

    Statements should be passed this way:
    [("SELECT * FROM friends WHERE age > %s", (42,)), ...]

    :param str statement: SQL statement
    :param tuple parameters: parameters of the SQL statement (optional)
    :param connection: a psycopg2 connection object (optional)
    :return list[tuple]: query results
    """
    if not connection:
        connection = get_connection(autocommit=True)
    cur = connection.cursor()
    try:
        cur.execute(statement, parameters)
        return cur.fetchall()
    except Exception as e:
        logging.critical(e)
        connection.rollback()
        raise
    finally:
        connection.close()


def table_exists(table_name):
    """
    Check if a table exists in the database.

    :param str table_name: name of the table to check
    :return bool: True if the table exists else False
    """
    result = query_sql(TABLE_EXISTS_SQL, (table_name,))
    return result[0][0]


def copy_from(
    file_name,
    table_name,
    delimiter=',',
    null_if='NULL',
    header=True,
    quote='"',
    encoding="utf-8",
):
    """
    Copy a CSV file into the specified table.

    :param str file_name: name of the file to load into the database
    :param str table_name: name of the table to load data into
    :param str delimiter: field delimiter of the file, optional (default ',')
    :param str null_if: textual representation of NULL values in the file,
                        optional (default 'NULL')
    :param bool header: whether the file contains a header with column names,
                        optional (default True)
    :param str quote: quote character of the file, optional (default '"')
    :param str encoding: file encoding, optional (default 'utf-8')
    """
    if header:
        template = COPY_FROM_WITH_HEADER_SQL
    else:
        template = COPY_FROM_NO_HEADER_SQL
    copy_command = sql.SQL(template).format(
        table_name=sql.Identifier(table_name),
        delimiter=sql.Literal(delimiter),
        null=sql.Literal(null_if),
        quote=sql.Literal(quote),
        encoding=sql.Literal(encoding),
    )

    connection = get_connection(autocommit=True)
    cur = connection.cursor()
    try:
        with open(file_name) as f:
            cur.copy_expert(copy_command, f)
    except Exception as e:
        logging.critical(e)
        connection.rollback()
        raise
    finally:
        connection.close()


def copy_to(
    file_name,
    table_name,
    delimiter=',',
    null_if='NULL',
    header=True,
    quote='"',
    encoding="utf-8",
):
    """
    Copy a table into a CSV file.

    :param str file_name: name of the file to unload the data into
    :param str table_name: name of the table to unload data from
    :param str delimiter: field delimiter of the file, optional (default ',')
    :param str null_if: textual representation of NULL values in the file,
                        optional (default 'NULL')
    :param bool header: whether the file contains a header with column names,
                        optional (default True)
    :param str quote: quote character of the file, optional (default '"')
    :param str encoding: file encoding, optional (default 'utf-8')
    """
    if header:
        template = COPY_TO_WITH_HEADER_SQL
    else:
        template = COPY_TO_NO_HEADER_SQL
    copy_command = sql.SQL(template).format(
        table_name=sql.Identifier(table_name),
        delimiter=sql.Literal(delimiter),
        null=sql.Literal(null_if),
        quote=sql.Literal(quote),
        encoding=sql.Literal(encoding),
    )

    connection = get_connection(autocommit=True)
    cur = connection.cursor()
    try:
        with open(file_name, "w") as f:
            cur.copy_expert(copy_command, f)
    except Exception as e:
        logging.critical(e)
        connection.rollback()
        raise
    finally:
        connection.close()
