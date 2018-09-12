import requests
from bs4 import BeautifulSoup
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os

logging.basicConfig(level='INFO')


def parse_html(html):
    """

    :param str html: HTML Raw string
    :return: BeautifulSoup
    """
    return BeautifulSoup(html, 'html.parser')


def google_search(city, keyword, page_number):
    """
    Search given keyword in google and returns result page links

    :param str city: Search city
    :param str keyword: Search Keyword
    :param int page_number: Google search page number
    :return: {
            'city': city,
            'keyword': keyword,
            'search_term': search_term,
            'page': page_number,
            'verified_links': [] Result links as array,
            'link_count': 0,
            'status_code': 200 # Status code
            'raw_links': []
    }
    """

    logging.debug("Searching from google with Key: {} City: {} and Page: {}".format(keyword, city, page_number))

    search_term = "{0} in {1}".format(keyword, city)
    url = \
        "https://www.google.com/search?q={search_term}&gl=us&hl=en&ei=9AuQW-CBHc_YsAXp0IOoCQ&start={page}&sa=N".format(
            search_term=search_term,
            page=page_number
        )
    response = requests.get(url)

    if response.status_code == 200:
        # If page response successful
        raw_links = []

        soup = parse_html(response.content)

        for i in soup.find_all('h3', {"class": 'r'}):
            raw_links.append(i.a['href'])

        verified_links = []

        for link in raw_links:
            verified_links.append(link.replace('/url?q=', ''))

        logging.info("Links extracted for search {}.".format(search_term))

        return {
            'city': city,
            'keyword': keyword,
            'search_term': search_term,
            'page': page_number,
            'verified_links': verified_links,
            'link_count': len(verified_links),
            'status_code': response.status_code
        }
    else:
        logging.error("Links can not extracted for search {}.".format(search_term))
        return {
            'city': city,
            'keyword': keyword,
            'search_term': search_term,
            'page': page_number,
            'verified_links': [],
            'link_count': 0,
            'status_code': response.status_code
        }


def download_site(site):
    """
    Downloads given site.
    :param str site: Web site url
    :return: Html String
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920x1080")

    chrome_driver = os.path.abspath('chromedriver')
    driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)
    driver.get(site)
    return driver.page_source
