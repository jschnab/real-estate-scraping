import csv

from configparser import ConfigParser


def remove_dollar_sign():
    config = ConfigParser()
    config.read("/home/jonathans/.browsing/browser.conf")
    columns = config["yelp"]["csv_header"].split(",")
    source = "/home/jonathans/real-estate-scraping/annotated_no_na.csv"
    target = "/home/jonathans/real-estate-scraping/annotated_clean.csv"

    with open(source) as infile:
        reader = csv.DictReader(infile)

        with open(target, "w") as outfile:
            writer = csv.DictWriter(outfile, columns)
            writer.writeheader()

            for row in reader:
                if row["common_charges"] != "NULL":
                    row["common_charges"] = row["common_charges"].lstrip("$")
                if row["monthly_taxes"] != "NULL":
                    row["monthly_taxes"] = row["monthly_taxes"].lstrip("$")
                writer.writerow(row)


if __name__ == "__main__":
    remove_dollar_sign()
