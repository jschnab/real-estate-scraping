import sys

from tor_sqs_browser import Browser

from nytimes.browse import *
from nytimes.parse_soup import *


def main():
    crawler = Browser(
        base_url="https://www.nytimes.com",
        stop_test=is_last_page,
        get_browsable=wrapper_next_page,
        get_parsable=get_listings,
        get_page_id=get_listing_id,
        soup_parser=parse_webpage,
        config_file="nytimes.conf",
    )
    if sys.argv[1] == "browse":
        crawler.browse(BEGIN_RENT_LISTINGS)
    elif sys.argv[1] == "harvest":
        crawler.harvest()
    elif sys.argv[1] == "extract":
        crawler.extract()


if __name__ == "__main__":
    main()
