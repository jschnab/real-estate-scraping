from selenium_browser import Browser


def main():
    crawler = Browser(
        base_url="https://www.cityrealty.com",
        config_file="cityrealty.conf",
    )
    crawler.harvest()


if __name__ == "__main__":
    main()
