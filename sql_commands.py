CREATE_CACHE_SQL = """
CREATE TABLE geocache (
    zip VARCHAR,
    burrough VARCHAR,
    address VARCHAR,
    latitude NUMERIC(8, 5),
    longitude NUMERIC(8, 5)
);"""

CHECK_CACHE_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM geocache
    WHERE zip = %s
    AND burrough = %s
    AND address = %s
);"""

INSERT_CACHE_SQL = """
INSERT INTO geocache (zip, burrough, address, latitude, longitude)
VALUES (%s, %s, %s, %s, %s);"""

QUERY_CACHE_SQL = """
SELECT latitude, longitude
FROM geocache
WHERE zip = %s AND burrough = %s AND address = %s;"""

CREATE_TABLE_RENTALS_SQL = """
CREATE TABLE rentals (
    listing_type VARCHAR,
    property_type VARCHAR,
    burrough VARCHAR,
    neighborhood VARCHAR,
    address VARCHAR,
    zip VARCHAR,
    price NUMERIC(9, 2),
    description VARCHAR,
    amenities VARCHAR,
    common_charges NUMERIC(9, 2),
    monthly_taxes NUMERIC(9, 2),
    days_listed INTEGER,
    size NUMERIC(9, 2),
    year_built INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    half_bathrooms NUMERIC(4, 1),
    rooms INTEGER,
    representative VARCHAR,
    agency VARCHAR,
    listing_id VARCHAR,
    source VARCHAR,
    collection_date DATE,
    latitude NUMERIC(8, 5),
    longitude NUMERIC(8, 5)
);"""

GET_PAST_BUSINESS_SQL = """
SELECT metrostations, buses, grocery, pharmacy, laundromat, collection_date
FROM rentals_annotated
WHERE zip = %s
AND burrough = %s
AND address = %s
ORDER BY collection_date DESC
LIMIT 1;"""

COPY_FROM_WITH_HEADER_SQL = """
COPY {table_name}
FROM STDIN
csv
DELIMITER {delimiter}
NULL {null}
HEADER
QUOTE {quote}
ENCODING {encoding};"""

COPY_FROM_NO_HEADER_SQL = """
COPY {table_name}
FROM STDIN
csv
DELIMITER {delimiter}
NULL {null}
QUOTE {quote}
ENCODING {encoding};"""

COPY_TO_WITH_HEADER_SQL = """
COPY {table_name}
TO STDOUT
csv
DELIMITER {delimiter}
NULL {null}
HEADER
QUOTE {quote}
ENCODING {encoding};"""

COPY_TO_NO_HEADER_SQL = """
COPY {table_name}
TO STDOUT
csv
DELIMITER {delimiter}
NULL {null}
QUOTE {quote}
ENCODING {encoding};"""

TABLE_EXISTS_SQL = """
SELECT EXISTS (
    SELECT FROM pg_tables
    WHERE tablename = %s
);"""
