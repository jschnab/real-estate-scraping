import os

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import shapefile

from mpl_toolkits.basemap import Basemap

HOME = str(Path.home())
CSV_PATH = os.path.join(
    HOME,
    "real-estate-scraping/cityrealty/samples/rentals.csv"
)
HERE = os.path.abspath(os.path.dirname(__file__))
SHAPEFILES = os.path.join(HERE, "shapefiles/nyc_burroughs")


DATA_TYPES = {
    "listing_type": str,
    "property_type": str,
    "burrough": str,
    "neighborhood": str,
    "address": str,
    "zip": str,
    "price": float,
    "description": str,
    "amenities": str,
    "common_charges": float,
    "monthly_taxes": float,
    "days_listed": float,
    "size": float,
    "year_built": float,
    "bedrooms": float,
    "bathrooms": float,
    "half_bathrooms": float,
    "rooms": float,
    "representative": str,
    "agency": str,
    "listing_id": str,
    "source": str,
    "latitude": float,
    "longitude": float,
}

PARSE_DATES = ["collection_date"]

MIN_LAT = 40.5
MAX_LAT = 41.5
MIN_LON = -74.3
MAX_LON = -73.6
MAX_PRICE = 10000


def read_csv(path):
    df = pd.read_csv(path, dtype=DATA_TYPES, parse_dates=PARSE_DATES)
    return df


def get_most_recent(df, date_column):
    """
    Get subset of dataframe corresponding to most recent date.

    :param pandas.DataFrame df: dataframe
    :param str date_column: name of the column containing dates
    :return pandas.DataFrame: most recent data
    """
    return df[df[date_column] == df[date_column].max()]


def plot_price_map(
    df,
    lat_col="latitude",
    lon_col="longitude",
    price_col="price",
):
    # filter extreme coordinates (errors) and price (outliers)
    filtered = df[
        (df[lat_col] > MIN_LAT) & (df[lat_col] < MAX_LAT) &
        (df[lon_col] > MIN_LON) & (df[lon_col] < MAX_LON) &
        (df[price_col] < MAX_PRICE)
    ]

    # draw map
    fig, ax = plt.subplots()
    shpfile = SHAPEFILES
    sf = shapefile.Reader(shpfile)
    x0, y0, x1, y1 = sf.bbox
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    m = Basemap(
        llcrnrlon=x0,
        llcrnrlat=y0,
        urcrnrlon=x1,
        urcrnrlat=y1,
        lat_0=cx,
        lon_0=cy,
        resolution="c",
        projection="mill",
    )
    m.drawmapboundary(linewidth=0)
    m.readshapefile(shpfile, "metro", linewidth=0.6)
    cmap = mpl.cm.jet
    norm = mpl.colors.Normalize(vmin=0, vmax=10000)
    sm = mpl.cm.ScalarMappable(norm=norm, cmap=cmap)
    _ = fig.colorbar(sm, ax=ax, shrink=0.8)
    m.scatter(
        filtered[lon_col].values,
        filtered[lat_col].values,
        latlon=True,
        alpha=0.2,
        c=filtered[price_col].values,
        cmap=plt.get_cmap("jet"),
        s=10,
    )
    plt.show()
