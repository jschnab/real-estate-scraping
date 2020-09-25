DEDUP_RENTALS = """
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY
      listing_type,
      neighborhood,
      address,
      year_built,
      bedrooms,
      agency
    ORDER BY
      listing_type,
      neighborhood,
      address,
      year_built,
      bedrooms,
      agency,
      collection_date
    DESC
  ) row_num
  FROM rentals
) t
WHERE row_num = 1;"""
