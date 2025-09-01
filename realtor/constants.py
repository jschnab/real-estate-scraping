import os

HERE = os.path.dirname(os.path.realpath(__file__))

DATA_DIR = "data"
DB_NAME = "realtor.db"
DB_PATH = os.path.join(HERE, DATA_DIR, DB_NAME)
PROPS_TABLE = "properties"
PROPS_FOR_SALE_TABLE = "for_sale"

BASE_URL = "https://www.realtor.com"
COOKIES_URL = "realestateandhomes-search/Lansing_MI/show-recently-sold"
COOKIES_PATH = os.path.join(HERE, "cookies")
