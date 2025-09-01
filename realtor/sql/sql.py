CREATE_TABLE_PROPERTIES = """
create or replace table {table_name} (
    property_id varchar
    , property_type varchar
    , sold_price int
    , price_reduced_amount int
    , beds int
    , baths_total int
    , baths_full int
    , baths_half int
    , year_built int
    , sqft int
    , lot_sqft int
    , sold_date date
    , street varchar
    , zipcode varchar
    , city varchar
    , state varchar
    , latitude decimal(12, 8)
    , longitude decimal(12, 8)
    , photos varchar[]
    , permalink varchar
    , source varchar
)
"""

INSERT_PROPERTIES_FROM_JSON = """
insert into {table_name} (
    property_id
    , property_type
    , sold_price
    , price_reduced_amount
    , beds
    , baths_total
    , baths_full
    , baths_half
    , year_built
    , sqft
    , lot_sqft
    , sold_date
    , street
    , zipcode
    , city
    , state
    , latitude
    , longitude
    , photos
    , permalink
    , source
)
select
    property_id
    , description.type
    , description.sold_price
    , price_reduced_amount
    , description.beds
    , description.baths
    , description.baths - description.baths_half
    , description.baths_half
    , description.year_built
    , description.sqft
    , description.lot_sqft
    , description.sold_date sold_date
    , location.address.line street
    , location.address.postal_code zipcode
    , location.address.city
    , location.address.state
    , location.address.coordinate.lat
    , location.address.coordinate.lon
    , list_append(
        list_append(
            list_transform(photos, x -> x.href),
            primary_photo.href
        ),
        location.street_view_url
    )
    , permalink
    , 'www.realtor.com'
from '{source_file}'
where concat(sold_date, street, zipcode)
    not in (select concat(sold_date, street, zipcode) from {table_name})
"""


CREATE_TABLE_PROPERTIES_FOR_SALE = """
create or replace table {table_name} (
    property_id varchar
    , status varchar
    , property_type varchar
    , list_price int
    , price_reduced_amount int
    , beds int
    , baths_total int
    , baths_full int
    , baths_half int
    , year_built int
    , sqft int
    , lot_sqft int
    , list_date timestamp
    , street varchar
    , zipcode varchar
    , city varchar
    , state varchar
    , latitude decimal(12, 8)
    , longitude decimal(12, 8)
    , is_foreclosure boolean
    , is_new_construction boolean
    , photos varchar[]
    , permalink varchar
    , source varchar
)
"""

INSERT_PROPERTIES_FOR_SALE_FROM_JSON = """
insert into {table_name} (
    property_id
    , status
    , property_type
    , list_price
    , price_reduced_amount
    , beds
    , baths_total
    , baths_full
    , baths_half
    , year_built
    , sqft
    , lot_sqft
    , list_date
    , street
    , zipcode
    , city
    , state
    , latitude
    , longitude
    , is_foreclosure
    , is_new_construction
    , photos
    , permalink
    , source
)
select
    property_id
    , case
        when flags.is_coming_soon is true then 'coming soon'
        when flags.is_pending is true then 'pending'
        when flags.is_contingent is true then 'contingent'
        else 'new listing'
    end status
    , description.type
    , list_price
    , price_reduced_amount
    , description.beds
    , description.baths
    , description.baths - description.baths_half
    , description.baths_half
    , description.year_built
    , description.sqft
    , description.lot_sqft
    , list_date
    , location.address.line street
    , location.address.postal_code zipcode
    , location.address.city
    , location.address.state
    , location.address.coordinate.lat
    , location.address.coordinate.lon
    , case when flags.is_foreclosure is true then true else false end
    , case when flags.is_new_construction is true then true else false end
    , list_append(
        list_append(
            list_transform(photos, x -> x.href),
            primary_photo.href
        ),
        location.street_view_url
    )
    , permalink
    , 'www.realtor.com'
from '{source_file}'
where concat(list_date, status, street, zipcode)
    not in (select concat(list_date, status, street, zipcode) from {table_name})
"""

HAVERSINE_MACRO = """
create or replace macro haversine(lat1, lon1, lat2, lon2) as
    2 * 6335 * asin(
        sqrt(
            pow(sin((radians(lat2) - radians(lat1)) / 2), 2)
            + cos(radians(lat1))
            * cos(radians(lat2))
            * pow(sin((radians(lon2) - radians(long2)) / 2), 2)
        )
    )
"""

TABLE_EXISTS = """
select count(*) = 1
from information_schema.tables
where table_name = '{table_name}'
"""

COUNT_RECORDS = """
select count(*) from {table_name}
"""

SIMILAR_LISTINGS = """
select
    street
    , sold_price
    , sold_date
    , beds
    , baths_full
    , baths_half
    , sqft
    , lot_sqft
    , year_built
from {table_name}
where 1=1
    and property_type = '{property_type}'
    and beds = {beds}
    and baths_full = {baths_full}
    and haversine(latitude, longitude, {lat}, {lon}) < 0.5
"""
"""
    and year_built between {year_built} - 5 and {year_built} + 5
    and baths_half = {baths_half}
    and sqft / {sqft} between 0.9 and 1.1
    and lot_sqft / {lot_sqft} between 0.9 and 1.1
"""


SIMILAR_LISTINGS_2 = """
select
    street
    , sold_price
    , sold_date
    , beds
    , baths_full
    , baths_half
    , sqft
    , lot_sqft
    , year_built
    , beds - {beds} beds_diff
    , baths_full - {baths_full} baths_diff
    , sqft - {sqft} sqft_diff
    , lot_sqft - {lot_sqft} lot_sqft_diff
    , year_built - {year_built} year_built_diff
    , round(haversine(latitude, longitude, {lat}, {lon}) * 1000)::int dist_m
from {table_name}
where 1=1
    and property_type = '{property_type}'
    and haversine(latitude, longitude, {lat}, {lon}) < 0.5
    and abs(year_built_diff) <= 20
    and beds_diff = 0
order by beds_diff, year_built_diff, sqft_diff, lot_sqft_diff, dist_m
"""
