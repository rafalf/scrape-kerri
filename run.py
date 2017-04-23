#!/usr/bin/env python


# win: pip install lxml==3.6.0  (other pip install lxml)
# pip install requests
# pip install beautifulsoup4

import requests
from requests import ConnectionError
from bs4 import BeautifulSoup
import os
import sys
import getopt
import logging
import time
import csv

scrape_url = 'https://www.kerrisdalecap.com/blog/page/{}/'
logger = logging.getLogger(os.path.basename(__file__))


def scrape(fld, from_date, to_date):

    logger.info('Scraping: {} for paging'.format(scrape_url.format("1")))
    r = requests.get(scrape_url.format("1"))

    # create download folder
    if not fld:
        downloads_folder = os.path.join(os.path.dirname(__file__), 'download')
    else:
        downloads_folder = os.path.join(os.path.dirname(__file__), fld)
    if not os.path.isdir(downloads_folder):
        os.mkdir(downloads_folder)

    soup = BeautifulSoup(r.text, 'lxml')

    # paging
    pages = soup.find("span", class_='pages')
    parsed_pages = pages.contents[0]
    pages_count = int(parsed_pages[-1:])
    logger.info('Found pages: {}'.format(pages_count))

    for page in range(1, pages_count + 1):

        r = requests.get(scrape_url.format(page))
        logger.info('Scraping: {} for data'.format(scrape_url.format(page)))

        soup = BeautifulSoup(r.text, 'lxml')
        blogs_section = soup.find("div", class_="blog-posts-section")

        for counter, post in enumerate(blogs_section.find_all("div", class_='each-post')):

            metadata = []

            heading = post.find('h2', class_='post-heading')
            heading_text = heading.contents[1].contents[0]

            logger.info('heading found: %s' %heading_text)

            ticker_found = heading_text.find('(')
            if ticker_found != -1:
                ticker_text = heading_text[ticker_found:]
                heading_text = heading_text[:ticker_found]
            else:
                ticker_text = ''

            metadata.append(heading_text.strip().encode("utf-8"))
            logger.info('heading add to metadata: %s' % heading_text)
            metadata.append(ticker_text)
            logger.info('ticker add to metadata: %s' % ticker_text)

            month = post.find('div', class_='post-month')
            month_text = month.contents[0]
            month_date = time.strptime(month_text, "%b")
            month_numeric = time.strftime("%m", month_date)
            logger.info('month numeric: %s' % month_numeric)

            day = post.find('div', class_='post-day')
            day_text = day.contents[0]

            year = post.find('div', class_='post-year')
            year_text = year.contents[0]

            date_ = day_text + ' ' + month_text + ' ' + year_text
            metadata.append(date_)
            logger.info('date add to metadata: %s' % date_)

            post_date = time.strptime(date_, '%d %b %Y')
            post_date_secs = time.mktime(post_date)
            logger.info('post date in secs: %s' % post_date_secs)

            if from_date < post_date_secs < to_date:
                logger.info('between start and end date --> processing')

                folder_struc = os.path.join(downloads_folder, year_text, month_numeric, day_text)
                if not os.path.isdir(folder_struc):
                    os.makedirs(folder_struc)
                    logger.info('folders created: %s' % folder_struc)

                ex_ = [excerpt_data for excerpt_data in blogs_section.find_all("div", class_='excerpt-data')]
                counter_data = ex_[counter]

                # -------------
                # grab hrefs
                # -------------
                div_hrefs = counter_data.find_all("div", class_="disclosure-report-all")
                div_href = div_hrefs[0]

                a = div_href.find('a')
                href = a.get('href')
                logger.info('href: {}'.format(href))

                if not href.count('javascript'):

                    for _ in range(10):
                        try:
                            # -------------
                            # get requests
                            # -------------
                            request = requests.get(href, timeout=30, stream=True)
                            file_ = os.path.join(folder_struc, 'article.pdf')
                            with open(file_, 'wb') as fh:
                                for chunk in request.iter_content(chunk_size=1024):
                                    fh.write(chunk)
                            logger.info('Web href: {}'.format(href))
                            logger.info('Redirect href: {}'.format(request.url))
                            logger.info('Downloaded as: {}'.format(file_))

                            metadata.append(href)
                            logger.info('web href add to metadata: %s' % href)
                            metadata.append(request.url)
                            logger.info('doc href add to metadata: %s' % request.url)
                            break
                        except ConnectionError:
                            logger.info('ConnectionError --> retry up to 10 times')
                    else:
                        logger.error('ERROR: Failed to download')

                else:
                    metadata.append('')

                _write_row(metadata, folder_struc)

            else:
                logger.info('not between start and end date --> skipping')


def _write_row(row, path_):
    w = os.path.join(path_, 'metadata.csv')
    with open(w, 'ab') as hlr:
        wrt = csv.writer(hlr, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        wrt.writerow(row)
        logger.debug('added to %s file: %s' % (w, row))


if __name__ == '__main__':
    dwn_folder = None
    verbose = None
    from_date = '05/20/2000'
    to_date = '05/20/2100'

    log_file = os.path.join(os.path.dirname(__file__), 'logs',
                                time.strftime('%d%m%y', time.localtime()) + "_scraper.log")
    file_hndlr = logging.FileHandler(log_file)
    logger.addHandler(file_hndlr)
    console = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(console)
    ch = logging.Formatter('[%(levelname)s] %(message)s')
    console.setFormatter(ch)
    file_hndlr.setFormatter(ch)

    argv = sys.argv[1:]
    opts, args = getopt.getopt(argv, "o:vf:t", ["output=", "verbose", "from=", "to="])
    for opt, arg in opts:
        if opt in ("-o", "--output"):
            dwn_folder = arg
        elif opt in ("-f", "--from"):
            from_date = arg
        elif opt in ("-t", "--to"):
            to_date = arg
        elif opt in ("-v", "--verbose"):
            verbose = True

    str_time = time.strptime(from_date, '%m/%d/%Y')
    from_in_secs = time.mktime(str_time)

    str_time = time.strptime(to_date, '%m/%d/%Y')
    to_in_secs = time.mktime(str_time)

    if verbose:
        logger.setLevel(logging.getLevelName('DEBUG'))
    else:
        logger.setLevel(logging.getLevelName('INFO'))

    logger.info('CLI args: {}'.format(opts))
    logger.info('from: {}'.format(from_date))
    logger.info('to: {}'.format(to_date))
    logger.debug('from_in_secs: {}'.format(from_in_secs))
    logger.debug('to_in_secs: {}'.format(to_in_secs))

    scrape(dwn_folder, from_in_secs, to_in_secs)