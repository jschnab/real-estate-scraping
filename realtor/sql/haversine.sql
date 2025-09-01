create or replace macro haversine(lat1, lon1, lat2, lon2) as
    2 * 6335 * asin(
        sqrt(
            pow(sin((radians(lat2) - radians(lat1)) / 2), 2)
            + cos(radians(lat1))
            * cos(radians(lat2))
            * pow(sin((radians(lon2) - radians(long2)) / 2), 2)
        )
    )
;
