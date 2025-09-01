insert into properties (
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
    , description.sold_date
    , location.address.line
    , location.address.postal_code
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
from 'data/properties.json';
