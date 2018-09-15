from typing import List

import requests
from bs4 import BeautifulSoup
import logging
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import os
import re
from urllib.parse import urlparse
from multiprocessing.pool import ThreadPool

logging.basicConfig(level='INFO')

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

    def __init__(self, city, keyword, search_term, page_number, links, status_code):
        self.city = city
        self.keyword = keyword
        self.search_term = search_term
        self.page_number = page_number
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

    logger.info(f"Searching from google with Key: {keyword} City: {city} and Page: {page_number}")

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

        logger.info(f"Links extracted for search {search_term}, Page: {page_number}.")

        return SearchResult(city=city, keyword=keyword, search_term=search_term, page_number=page_number,
                            links=links, status_code=response.status_code)
    else:
        logger.error("Links can not extracted for search {}.".format(search_term))
        return SearchResult(city=city, keyword=keyword, search_term=search_term, page_number=page_number,
                            links=[], status_code=response.status_code)


def download_page(url):
    """
    Downloads given site.
    :param str url: Web site url
    :return: Html String
    :rtype Dict[str, str]
    """

    data = {
        'url': url,
        'html': None,
        'successful': False
    }

    logger.info(f"Downloading page: {url}")

    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")

        chrome_driver = os.path.abspath('chromedriver')
        driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)
        driver.get(url)
        data['html'] = driver.page_source
        data['successful'] = True
        driver.close()
    except TimeoutException:
        logger.warning(f"Timeout for downloading page: {url}")
    except:
        raise

    return data


def extract_emails(html):
    """
    Extract emails from html.

    :param html: Html page
    :return: List[str] Unique list of mails.
    """

    mails = []
    result = list(set(re.findall('[\w\.-]+@[\w\.-]+\.[\w\.-]+', html)))
    for mail in result:  # Clear image links
        if not (mail.endswith('png') or mail.endswith('jpg')):
            mails.append(mail)
    return mails


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
        "about.php",
        "support"
    ]

    links = []
    for ptr in patterns:
        url = site
        url += ptr if url.endswith('/') else f"/{ptr}"
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


def download_pages(pages):
    """
    Download html sources for given pages.
    :param pages: List of site pages
    :return Html sources of site pages
    :rtype: List[Dict[str, str]]
    """

    if len(pages) == 0:
        return []

    pool = ThreadPool(processes=10)
    threads = []

    html_sources: List[str] = []

    logger.debug(f"Downloading {len(pages)} pages. [{pages[0][:10]}, ...]")
    for page in pages:
        thread = pool.apply_async(download_page, (page,))
        threads.append(thread)

    for thread in threads:
        result = thread.get()
        html_sources.append(result)

    return html_sources


def check_url_exists(url, path='', current=0, max_redirect=5):
    """
    Checks given url page exists
    :param str url:
    :param str path: Current path
    :param int max_redirect: Maximum redirect cycle
    :param int current: Current redirect cyle
    :returns: dict
    """

    logger.debug(f"Checking url {url} with path: {path}. Max: {max_redirect}, Current: {current}")

    if current > max_redirect:
        logger.warning(f"Max redirect reached for {url}")
        return {'url': url, 'exists': False}

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }

    try:
        response = requests.head(url + path, headers=headers)
        logger.debug(f"Url checked. Status: {response.status_code}, URL: {url}")
    except:
        logger.warning(f"URL could not checked.")
        return {'url': url, 'exists': False}

    # If page redirects follow the link
    if response.status_code == 302:
        return check_url_exists(url=url, path=response.headers.get('Location'),
                                max_redirect=max_redirect, current=current + 1)

    # Page moved
    if response.status_code == 301:
        return check_url_exists(url=response.headers.get('Location'),
                                max_redirect=max_redirect, current=current + 1)
    return {'url': url, 'exists': response.status_code == 200}


def get_exist_pages(urls):
    """

    :param List[str] urls: Site pages
    :return: Dict[str, str]
            result['exists'] -- boolean
            result['url']
    """
    pool = ThreadPool(processes=10)

    threads = []
    results = []

    for url in urls:
        thread = pool.apply_async(check_url_exists, (url,))
        threads.append(thread)

    for thread in threads:
        results.append(thread.get(timeout=10))

    exists = []

    # Filter only exists ones
    for result in results:  # [{url: .., exists: ...}, {}]
        if result.get('exists'):
            exists.append(result.get('url'))
    return list(set(exists))


def download_site_pages(site_url):
    logger.info(f"Downloading pages for: {site_url}")

    sub_pages = generate_contact_urls(site_url)  # Generate sub pages
    logger.info(f"Sub pages generated for {site_url}")
    exist_pages = get_exist_pages(sub_pages)  # Filter exist pages
    logger.info(f"Existing sub pages count for {site_url} is {len(exist_pages)}.")

    if site_url not in exist_pages:  # If site url not in exist pages add it
        exist_pages.append(site_url)

    # Download pages html source
    page_sources = download_pages(exist_pages)  # [{url: ..., successful: ..., html: ...}]

    logger.info(f"Downloaded site pages successfully: {site_url}")
    return {
        'site': site_url,
        'pages': page_sources
    }


def extract_emails_from_sources(site):
    """

    :param site: Dict
        site['site']: str
        site['pages']: List[{url: ..., successful: ..., html: ...}]
    :return:
    """

    mails = []
    logger.info(f"Extracting mails for: {site.get('site')}")
    for page in site.get('pages'):  # For every page in site pages
        if page.get('successful'):  # If page downloaded successfully
            mails.extend(extract_emails(page.get('html')))  # Extract mails from page html

    # After all pages extracted
    unique_mails = list(set(mails))  # Remove same mails from list
    return {
        'site': site.get('site'),
        'mails': unique_mails,
        'count': len(unique_mails)
    }


def process_search_result_site(url, keyword, city, search_term, page_number):
    """
    Process one site, generated sub pages and extract mails
    :param str url: Site url
    :param str keyword: search keyword
    :param str city: search city
    :param str search_term: search term
    :param int page_number: google result page number
    :return:
    """
    logger.info(f"Processing page {url}. Keyword: {keyword}, City: {city}, Page: {page_number}")
    sources = download_site_pages(url)  # {site: ..., pages: [{url: ..., successful: ..., html: ...}]}
    mail_result = extract_emails_from_sources(sources)  # {site: ..., mails: ...., count: ...}
    return {
        'site': url,
        'keyword': keyword,
        'city': city,
        'search_term': search_term,
        'page_number': page_number,
        'mails': mail_result.get('mails'),
        'mail_count': mail_result.get('count')
    }


def main(keywords, cities, start_page, end_page):
    """
    :param List[str] keywords:
    :param List[str} cities:
    :param int start_page:
    :param int end_page:
    """

    all_search_results = []  # Store all search results

    for keyword in keywords:  # For every keyword
        for city in cities:  # For every city
            # Collect search results from city-keyword pair
            search_results = download_search_result_pages(city=city, keyword=keyword, start_page=start_page,
                                                          end_page=end_page)
            all_search_results.extend(search_results)  # Extends to all search results

    downloaded_sites = []

    results = []  # [{site: .., keyword: ..., city: ..., page_number: ..., search_term: ...}]

    for search_result in all_search_results:  # For every google search result
        for site_url in search_result.links:  # For every site url in search result
            if site_url not in downloaded_sites:
                results.append(process_search_result_site(
                    url=site_url,
                    keyword=search_result.keyword,
                    city=search_result.city,
                    search_term=search_result.search_term,
                    page_number=search_result.page_number
                ))
                downloaded_sites.append(site_url)

    return results


if __name__ == '__main__':
    main(keywords=['Dentist'], cities=['New York'], start_page=6, end_page=7)
