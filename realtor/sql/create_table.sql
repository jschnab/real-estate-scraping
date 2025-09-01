create or replace table properties (
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
);
