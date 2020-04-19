CHECK_CACHE_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM geocache
    WHERE postcode = %s
    AND city = %s
    AND address = %s
);"""

INSERT_CACHE_SQL = """
INSERT INTO geocache (postcode, city, address, lat, lon)
VALUES (%s, %s, %s, %s, %s);"""

QUERY_CACHE_SQL = """
SELECT lat, lon
FROM geocache
WHERE postcode = %s AND city = %s AND address = %s;"""
