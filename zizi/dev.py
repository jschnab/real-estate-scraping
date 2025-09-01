import bs4
from bs4 import BeautifulSoup
from collections import deque
from configparser import ConfigParser
from itertools import chain, cycle
import numpy as np
from random import choice
import re
import requests
import time


def get_user_agents():
    """
    Return list of user agents for http requests from a text file.

    :return List: list of common user agents
    """
    with open('user_agents.txt') as f:
        lines = f.readlines()
    user_agents = [l.strip() for l in lines]
    return user_agents


def get_headers(user_agents, proxies):
    """
    Return a list of headers for HTTP requests.

    :param user_agents List[str]: list of user agents
    :param proxies List[str]: list of proxies, need their number
    :return List[Dict]: headers
    """
    conf = ConfigParser()
    conf.read('headers.ini')
    headers = {
        'Accept': conf['DEFAULT']['accept'],
        'Accept-Encoding': conf['DEFAULT']['accept_encoding'],
        'Accept-Language': conf['DEFAULT']['accept_language']
    }
    headers_list = [headers] * len(proxies)
    user_agents_cycle = cycle(user_agents)
    for h in headers_list:
        h['User-Agent'] = next(user_agents_cycle)
    return headers_list


def get_proxies():
    """
    Return the list of proxies for HTTP requests.

    :return List[Dict]: list of proxies following the format
                        {'protocol': 'http://ip:port'}
    """
    proxies = []
    with open('proxies.txt') as f:
        lines = f.readlines()
    proxies = [{'https': f'https://{proxy.strip()}'} for proxy in lines]
    return proxies


def get_page_contents(url, headers, timeout, proxy):
    """
    Get contents of web page from Zillow.

    :param str url: URL of the web page
    :param Dict headers: HTTP GET request headers
    :param int timeout: timeout for the requests (seconds)
    :param Dict proxy: dictionary for proxies in format
                         {'protocol':'http://ip:port'}
    :return str: page contents
    """
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
            proxies=proxy)
    except Exception as e:
        print(e)
        return
    return response.text


def get_listings(page_contents):
    """
    Get list of properties to rent from a Zillow URL.

    :param str page_contents: contents of web page
    :return List: list of BeautifulSoup objects
    """
    soup = BeautifulSoup(page_contents, 'html.parser')
    listings = soup.find_all('', {'class': 'list-card-link list-card-info'})
    return listings


def get_address(prop):
    """
    Get address from BeautifulSoup object representing a single property.

    :param prop: BeautifulSoup object
    :return str: address
    """
    address = prop.find('h3', {'class': 'list-card-addr'}).text
    return address


def get_price(prop):
    """
    Get price from BeautifulSoup object representing a single property.

    :param prop: BeautifulSoup object
    :return str: price
    """
    price = prop.find('div', {'class': 'list-card-price'}).text
    return price


def get_description(prop):
    """
    Get description from BeautifulSoup object representing a single property.

    :param prop: BeautifulSoup object
    :return str: description of the property
    """
    description = prop.find('div', {'class': 'list-card-type'}).text
    return description


def get_link(prop):
    """
    Get link from BeautifulSoup object representing a single property.

    :param prop: BeautifulSoup object
    :return str: link to the property details
    """
    return prop.attrs['href']


def get_details(prop):
    """
    Get number of bedrooms, bathrooms and surface (square feet) of the
    property.

    :param prop: BeautifulSoup Tag object
    :return Tuple: (# bedrooms, # bathrooms, surface)
    """
    details = prop.find('', {'class': 'list-card-details'})
    children = [c.children for c in list(details.children)]
    children = list(chain.from_iterable(children))[::-1]
    results = (np.nan, np.nan, np.nan)
    for i in range(len(children) - 1):
        if isinstance(bs4.element.Tag):
            if children[i].text.strip() == 'sqft':
                results[2] = to_int(children[i + 1])
            elif children[i].text.strip() == 'ba':
                results[1] = to_int(children[i + 1])
            elif children[i].text.strip() == 'bd':
                results[0] = to_int(children[i + 1])
    return results


def is_valid(link):
    """
    Determines if a link should be followed when crawling the web site.

    :param link bs4.element.Tag: BeautifulSoup object to check
    :return bool: True if the link should be followed, else False
    """
    if link.attrs.get('class') == ['list-card-link', 'list-card-info']:
        return True
    if re.search('.*new-york-ny.*', link.attrs.get('href', '')):
        return True
    return False


def get_links(url, headers_proxies, timeout, count):
    time.sleep(0.3)
    if count == 999:
        return
    count += 1
    global pages
    if url[0] == '/':
        url = f'https://www.zillow.com{url}'
    hp = choice(headers_proxies)
    contents = get_page_contents(url, hp['header'], timeout, hp['proxy'])
    if contents is not None:
        soup = BeautifulSoup(contents, 'html.parser')
        for link in soup.find_all('a'):
            if is_valid_link(link):
                new_page = link.attrs['href']
                pages.add(new_page)
                get_links(new_page, headers_proxies, timeout, count)
    else:
        return


def crawl_dfs(start, headers_proxies, timeout=20):
    """
    Crawl through Zillow using depth-first search.

    :param start str: URL where to start crawling
    :param headers_proxies List[Dict]: list of dictionaries where one key
                                      contains a proxy and the other key
                                      contains HTTP headers
    :param timeout int: timeout for HTTP GET requests (seconds)
    :return set: set of URL found during crawling
    """
    to_explore = deque([start])
    explored = set(start)

    while to_explore:
        current = to_explore.pop()
        if current[0] == '/':
            current = f'https://www.zillow.com{current}'

        identity = choice(headers_proxies)
        contents = get_page_contents(
            current,
            identity['header'],
            timeout,
            identity['proxy'])

        if contents is not None:
            soup = BeautifulSoup(contents, 'html.parser')
            links = [link for link in soup.find_all('a') if is_valid(link)]
            for link in links:
                new_page = link.attrs['href']
                if new_page in explored:
                    continue
                print(new_page)
                to_explore.append(new_page)
                explored.add(new_page)

    return explored


def crawl_bfs(start, headers_proxies, timeout=20):
    """
    Crawl through Zillow using breadth-first search.

    :param start str: URL where to start crawling
    :param headers_proxies List[Dict]: list of dictionaries where one key
                                      contains a proxy and the other key
                                      contains HTTP headers
    :param timeout int: timeout for HTTP GET requests (seconds)
    :return set: set of URL found during crawling
    """
    to_explore = deque([start])
    explored = set(start)

    while to_explore:
        current = to_explore.popleft()
        if current[0] == '/':
            current = f'https://www.zillow.com{current}'

        identity = choice(headers_proxies)
        contents = get_page_contents(
            current,
            identity['header'],
            timeout,
            identity['proxy'])

        if contents is not None:
            soup = BeautifulSoup(contents, 'html.parser')
            links = [link for link in soup.find_all('a') if is_valid(link)]
            for link in links:
                new_page = link.attrs['href']
                if new_page in explored:
                    continue
                print(new_page)
                to_explore.append(new_page)
                explored.add(new_page)

    return explored



if __name__ == '__main__':
    proxies = get_proxies()
    user_agents = get_user_agents()
    headers = get_headers(user_agents, proxies)
    hp = [{'proxy': x, 'header': y} for x, y in zip(proxies, headers)]

    pages = crawl_bfs(
        'https://www.zillow.com/new-york-ny/cheap-apartments/',
        headers_proxies=hp)

    with open('output.txt', 'w') as f:
        for p in pages:
            f.write(p + '\n')
