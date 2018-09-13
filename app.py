import requests
from bs4 import BeautifulSoup
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import re
from urllib.parse import urlparse
from multiprocessing.pool import ThreadPool

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
            verified_links.append(clean_site_url(link.replace('/url?q=', '')))

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


def extract_emails(html):
    """
    Extract emails from html.

    :param html: Html page
    :return: List[str] Unique list of mails.
    """

    return list(set(re.findall('[\w\.-]+@[\w\.-]+\.[\w\.-]+', html)))


def find_domain_mails(domain, mails):
    """

    :param str domain: web site domain. Ex: example.com
    :param List[str] mails: Mail list
    :return: List[str]
    """

    domain_mails = []
    for mail in mails:
        if mail.endswith(domain):
            domain_mails.append(mail)
    return domain_mails


def generate_contact_urls(site):
    """
    Generates site urls for concat pages.

    :param str site: Site url. Example: http://example.com
    :return: [
        "http://example.com/contact",
    ]
    """

    patterns = [
        "contact",
        "contact-us",
        "contactus",
        "contact.html",
        "contact.php",
        "about",
        "aboutus",
        "about-us",
        "about.html",
        "about.php"
    ]

    links = []
    for ptr in patterns:
        url = "{site}/{pattern}".format(site=site, pattern=ptr)
        links.append(url)
    return links


def clean_site_url(url):
    """
    Remove any query parameters from url

    :param str url: Site url. Example: http://example.com/?a=b&asdad?
    :return: http://example.com
    """
    return '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))


def process_site(site_url):
    """
    Download site and sub page of the sites than extract emails.

    :param str site_url: Site url
    :return: List[str]
    """

    pages = [site_url]
    contact_urls = generate_contact_urls(site_url)
    pages.extend(contact_urls)


def main(city, keyword, start_page=1, end_page=20):
    """

    :param city: Search city
    :param keyword: Search keyword
    :return:
    """

    web_site_links = []

    for i in range(start_page, end_page):
        result = google_search(city, keyword, i)
        if result.get('link_count') > 0:
            web_site_links.extend(result.get('verified_links'))

    for site in web_site_links:
        process_site(site)
