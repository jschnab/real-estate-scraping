import csv
import logging
import math
import os
import sys

sys.path.insert(0, "..")

from craigslist.parse_soup import string_to_float

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO)


def parse_coordinates_from_description(description):
    """
    This function parses the latitude and longitude of a home rental ad,
    which were added to the ad description for convenience.

    :param str description: rental ad description
    :returns: tuple[float, float] - latitude and longitude
    """
    coordinates = description.split()[-1]
    latitude = string_to_float(coordinates.split(";")[0].split(":")[1])
    longitude = string_to_float(coordinates.split(";")[1].split(":")[1])
    if math.isnan(latitude):
        latitude = "NULL"
    if math.isnan(longitude):
        longitude = "NULL"
    return latitude, longitude


def add_coordinates(
    input_csv,
    output_csv,
    columns,
):
    """
    Copy a CSV file and add geolocation coordinates from the address.

    :param str input_csv: name of CSV file containing only the address
    :param str output_csv: name of CSV file with added latitude and longitude
    :param list[str] columns: columns of the output CSV file
    """
    with open(input_csv) as infile:
        reader = csv.DictReader(infile, lineterminator=os.linesep)

        with open(output_csv, "w") as outfile:
            writer = csv.DictWriter(
                outfile,
                columns,
                lineterminator=os.linesep,
            )
            writer.writeheader()

            for row in reader:
                description = row["description"]
                if description != "NULL":
                    lat, lon = parse_coordinates_from_description(description)
                else:
                    lat, lon = "NULL", "NULL"
                row["latitude"] = lat
                row["longitude"] = lon
                writer.writerow(row)
