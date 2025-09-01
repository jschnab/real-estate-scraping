import os
import sys

import duckdb

import constants as cst
from sql import sql


def similar_listings(
    property_type,
    year_built,
    beds,
    baths_full,
    baths_half,
    sqft,
    lot_sqft,
    lat,
    lon,
    zipcode,
):
    query = sql.SIMILAR_LISTINGS_2.format(
        table_name=cst.PROPS_TABLE,
        property_type=property_type,
        year_built=year_built,
        beds=beds,
        baths_full=baths_full,
        baths_half=baths_half,
        sqft=sqft,
        lot_sqft=lot_sqft,
        lat=lat,
        lon=lon,
        zipcode=zipcode,
    )
    with duckdb.connect(cst.DB_PATH) as con:
        res = con.sql(query)
        print(res)


def main():
    similar_listings(
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
        sys.argv[4],
        sys.argv[5],
        sys.argv[6],
        sys.argv[7],
        sys.argv[8],
        sys.argv[9],
        sys.argv[10],
    )


if __name__ == "__main__":
    main()
