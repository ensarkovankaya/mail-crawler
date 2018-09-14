from typing import List

import requests
from bs4 import BeautifulSoup
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import re
from urllib.parse import urlparse
from multiprocessing.pool import ThreadPool

logging.basicConfig(level='DEBUG')

logger = logging.getLogger('app')


def set_log_level(level):
    """
    Changes log level
    :param str level:
    :return:
    """
    logger.setLevel(level)


def parse_html(html):
    """

    :param str html: HTML Raw string
    :return: BeautifulSoup
    """
    return BeautifulSoup(html, 'html.parser')


class SearchResult:

    def __init__(self, city, keyword, search_term, page, links, status_code):
        self.city = city
        self.keyword = keyword
        self.search_term = search_term
        self.page = page
        self.status_code = status_code
        self.links = list(set(links))
        self.count = len(self.links)

    def is_valid(self):
        return self.status_code == 200

    def is_link_exists(self, link):
        """
        Checks is link exists itself
        :param str link: Page link
        :rtype: bool
        """
        return link in self.links

    def remove_link(self, link):
        """
        Remove given link from itself if exists
        :param str link: Page link
        """

        if link in self.links:
            self.links.remove(link)
            self.count = len(self.links)
            logger.debug(f"Link removed: {link}")
        else:
            logger.warning(f"Link not exists: {link}")


def google_search(city, keyword, page_number):
    """
    Search given keyword in google and returns result page links

    :param str city: Search city
    :param str keyword: Search Keyword
    :param int page_number: Google search page number
    :rtype: SearchResult
    """

    logger.debug("Searching from google with Key: {} City: {} and Page: {}".format(keyword, city, page_number))

    search_term = "{0} in {1}".format(keyword, city)
    url = \
        "https://www.google.com/search?q={search_term}&gl=us&hl=en&ei=9AuQW-CBHc_YsAXp0IOoCQ&start={page}&sa=N".format(
            search_term=search_term,
            page=page_number
        )
    response = requests.get(url)
    logger.debug(f"Search result page successfully for search '{search_term}' in page {page_number}.")

    if response.status_code == 200:
        # If page response successful
        raw_links = []

        soup = parse_html(response.content)

        for i in soup.find_all('h3', {"class": 'r'}):
            raw_links.append(i.a['href'])

        links = []

        for link in raw_links:
            links.append(clean_site_url(link.replace('/url?q=', '')))

        logger.info("Links extracted for search {}.".format(search_term))

        return SearchResult(city=city, keyword=keyword, search_term=search_term, page=page_number,
                            links=links, status_code=response.status_code)
    else:
        logger.error("Links can not extracted for search {}.".format(search_term))
        return SearchResult(city=city, keyword=keyword, search_term=search_term, page=page_number,
                            links=[], status_code=response.status_code)


def download_site(site):
    """
    Downloads given site.
    :param str site: Web site url
    :return: Html String
    :rtype Dict[str, str]
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920x1080")

    chrome_driver = os.path.abspath('chromedriver')
    driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)
    driver.get(site)
    data = {
        'site': site,
        'html': driver.page_source
    }
    driver.close()
    return data


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


def download_search_result_pages(city, keyword, start_page, end_page):
    """
    Makes google search for given search term and collects all search results.

    :param str city: Search city
    :param str keyword: Search term
    :param int start_page:
    :param int end_page:
    :rtype: List[SearchResult]
    """
    search_results: List[SearchResult] = []
    pool = ThreadPool(processes=10)

    threads = []

    for page_number in range(start_page, end_page + 1):
        thread = pool.apply_async(google_search, (city, keyword, page_number))
        threads.append(thread)

    for thread in threads:
        result = thread.get(timeout=10)
        search_results.append(result)

    return search_results


def download_site_pages(site_url):
    """
    Generates given site sub pages and download html sources.
    :param site_url: Site URL
    :return Html sources of site pages
    :rtype: List[Dict[str, str]]
    """

    pages = [site_url]
    pages.extend(generate_contact_urls(site_url))

    pool = ThreadPool(processes=10)
    threads = []

    html_sources: List[str] = []

    for page in pages:
        thread = pool.apply_async(download_site, (page,))
        threads.append(thread)

    for thread in threads:
        result = thread.get()
        html_sources.append(result)

    return html_sources


def main(keywords, cities, start_page, end_page):
    """
    :param List[str] keywords:
    :param List[str} cities:
    :param int start_page:
    :param int end_page:
    """

    all_search_results = []

    for keyword in keywords:
        for city in cities:
            search_results = download_search_result_pages(city=city, keyword=keyword, start_page=start_page,
                                                          end_page=end_page)
            all_search_results.extend(search_results)

    downladed_sites = []

    all_sources = []

    for search_result in all_search_results:
        for page in search_result.links:
            if page not in downladed_sites:
                sources = download_site_pages(page)
                all_sources.extend(sources)
    return all_sources
