#!/usr/bin/env python3

import os
import sys

import duckdb

import constants as cst
import sql.sql as sql

HERE = os.path.dirname(os.path.realpath(__file__))
DB_PATH = os.path.join(HERE, cst.DATA_DIR, cst.DB_NAME)


def table_exists(table_name, con):
    return con.execute(
        sql.TABLE_EXISTS.format(table_name=table_name)
    ).fetchone()[0]


def number_records(table_name, con):
    return con.execute(
        sql.COUNT_RECORDS.format(table_name=table_name)
    ).fetchone()[0]


def insert_properties(
    source_file,
    table_name=cst.PROPS_TABLE,
    db_path=DB_PATH,
):
    with duckdb.connect(db_path) as con:
        n_recs_before = number_records(table_name, con)
        resp = con.sql(
            sql.INSERT_PROPERTIES_FROM_JSON.format(
                table_name=table_name,
                source_file=source_file,
            )
        )
        n_recs_after = number_records(table_name, con)
    print(f"Inserted {n_recs_after - n_recs_before} records into {table_name}")


def insert_properties_for_sale(
    source_file,
    table_name=cst.PROPS_FOR_SALE_TABLE,
    db_path=DB_PATH,
):
    with duckdb.connect(db_path) as con:
        if not table_exists(table_name, con):
            con.sql(
                sql.CREATE_TABLE_PROPERTIES_FOR_SALE.format(
                    table_name=table_name
                )
            )
        n_recs_before = number_records(table_name, con)
        resp = con.sql(
            sql.INSERT_PROPERTIES_FOR_SALE_FROM_JSON.format(
                table_name=table_name,
                source_file=source_file,
            )
        )
        n_recs_after = number_records(table_name, con)
    print(f"Inserted {n_recs_after - n_recs_before} records into {table_name}")



def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path>")
        return
    source_path = sys.argv[1]
    insert_properties(source_path)


if __name__ == "__main__":
    main()
