import os

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import shapefile

from matplotlib.ticker import FormatStrFormatter, MultipleLocator
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
MAX_BEDROOM = 10
MAX_BATHROOM = 10
MAX_ROOM = 10
MIN_SIZE = 100
MIN_YEAR_BUILT = 1800


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


def filter_extremes(df):
    price_filter = df["price"] < MAX_PRICE
    bedroom_filter = df["bedrooms"] < MAX_BEDROOM
    bathroom_filter = df["bathrooms"] < MAX_BATHROOM
    room_filter = df["rooms"] < MAX_ROOM
    size_filter = df["size"] > MIN_SIZE
    year_built_filter = df["year_built"] > MIN_YEAR_BUILT
    filtered = df[
        price_filter &
        bedroom_filter &
        bathroom_filter &
        room_filter &
        size_filter &
        year_built_filter
    ]
    return filtered


def plot_price_map(
    df,
    lat_col="latitude",
    lon_col="longitude",
    price_col="price",
    figsize=(12, 8.5),
):
    # filter extreme coordinates (errors) and price (outliers)
    filtered = df[
        (df[lat_col] > MIN_LAT) & (df[lat_col] < MAX_LAT) &
        (df[lon_col] > MIN_LON) & (df[lon_col] < MAX_LON) &
        (df[price_col] < MAX_PRICE)
    ]

    # draw map
    fig, ax = plt.subplots(figsize=figsize)
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
    cbar = fig.colorbar(sm, ax=ax, shrink=0.8)
    cbar.ax.tick_params(labelsize=16)
    m.scatter(
        filtered[lon_col].values,
        filtered[lat_col].values,
        latlon=True,
        alpha=0.2,
        c=filtered[price_col].values,
        cmap=plt.get_cmap("jet"),
        s=figsize[0],
    )
    plt.show()


def plot_histograms(df):
    fig, axs = plt.subplots(2, 3, figsize=(9, 6))

    # bedrooms
    axs[0, 0].hist(
        df[df["bedrooms"] < MAX_BEDROOM]["bedrooms"],
        bins=list(range(1, 11)),
        align="left",
    )
    axs[0, 0].xaxis.set_major_locator(MultipleLocator(2))
    axs[0, 0].xaxis.set_major_formatter(FormatStrFormatter("%d"))
    axs[0, 0].xaxis.set_minor_locator(MultipleLocator(1))
    axs[0, 0].set_title("bedrooms")

    # bathrooms
    axs[0, 1].hist(
        df[df["bathrooms"] < MAX_BATHROOM]["bathrooms"],
        bins=list(range(1, 11)),
        align="left",
    )
    axs[0, 1].xaxis.set_major_locator(MultipleLocator(2))
    axs[0, 1].xaxis.set_major_formatter(FormatStrFormatter("%d"))
    axs[0, 1].xaxis.set_minor_locator(MultipleLocator(1))
    axs[0, 1].set_title("bathrooms")

    # rooms
    axs[0, 2].hist(
        df[df["rooms"] < MAX_ROOM]["rooms"],
        bins=list(range(1, 11)),
        align="left",
    )
    axs[0, 2].xaxis.set_major_locator(MultipleLocator(2))
    axs[0, 2].xaxis.set_major_formatter(FormatStrFormatter("%d"))
    axs[0, 2].xaxis.set_minor_locator(MultipleLocator(1))
    axs[0, 2].set_title("rooms")

    # year built
    axs[1, 0].hist(df[df["year_built"] > MIN_YEAR_BUILT]["year_built"])
    axs[1, 0].xaxis.set_major_locator(MultipleLocator(100))
    axs[1, 0].xaxis.set_major_formatter(FormatStrFormatter("%d"))
    axs[1, 0].xaxis.set_minor_locator(MultipleLocator(25))
    axs[1, 0].set_title("year built")

    # price
    axs[1, 1].hist(df[df["price"] < MAX_PRICE]["price"])
    axs[1, 1].xaxis.set_major_locator(MultipleLocator(5000))
    axs[1, 1].xaxis.set_major_formatter(FormatStrFormatter("%d"))
    axs[1, 1].xaxis.set_minor_locator(MultipleLocator(1000))
    axs[1, 1].set_title("price")

    # size
    axs[1, 2].hist(df[df["size"] > MIN_SIZE]["size"])
    axs[1, 2].xaxis.set_major_locator(MultipleLocator(250))
    axs[1, 2].xaxis.set_major_formatter(FormatStrFormatter("%d"))
    axs[1, 2].xaxis.set_minor_locator(MultipleLocator(125))
    axs[1, 2].set_title("size")

    plt.subplots_adjust(wspace=0.3, hspace=0.25)
    plt.show()


def price_boxplot(df):
    ny = df[(df["burrough"] == "New York") & (df["price"] < MAX_PRICE)]
    bk = df[(df["burrough"] == "Brooklyn") & (df["price"] < MAX_PRICE)]
    qe = df[(df["burrough"] == "Queens") & (df["price"] < MAX_PRICE)]
    bx = df[(df["burrough"] == "Bronx") & (df["price"] < MAX_PRICE)]
    st = df[(df["burrough"] == "Staten Island") & (df["price"] < MAX_PRICE)]

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.boxplot(
        [ny["price"], bk["price"], qe["price"], bx["price"], st["price"]],
        notch=True,
        labels=["New York", "Brooklyn", "Queens", "Bronx", "Staten Island"],
    )
    ax.set_ylabel("Price ($)", fontsize=16)
    ax.set_yticks(list(range(0, 10000, 1000)))
    for side in ["top", "bottom", "left", "right"]:
        ax.spines[side].set_visible(False)
    plt.tick_params(axis="x", bottom=False)
    plt.grid(axis="y", linestyle=":")
    plt.show()


if __name__ == "__main__":
    df = read_csv(CSV_PATH)
    recent = get_most_recent(df, "collection_date")
    plot_price_map(recent)
