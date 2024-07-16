from os.path import dirname, join, exists
from typing import Optional
from time import sleep
import random
from tqdm import tqdm
from pathlib import Path
from pydantic import BaseModel
import re
import threading
from loguru import logger
import requests
from bs4 import BeautifulSoup



class ScrapeThread(threading.Thread):
    def __init__(self, disease_page):
        threading.Thread.__init__(self)
        self.disease_page = disease_page
    
    def run(self):
        list_url = get_pages(self.disease_page)
        for url in tqdm(list_url, desc=f'crawling {self.disease_page.disease}'):
            get_page_content(url)
        



CORPUS_LOCATION = join(dirname(dirname(__file__)), "datasets", "crawl")

class URLModel(BaseModel):
    disease: str
    url: str
    date: Optional[str] = None
    title: Optional[str] = None

BASE_URL = 'https://vnvc.vn/thong-tin-benh-hoc'

def get_urls():
    categories = {
        'disiase': 62
    }
    for category in categories.keys():
        for i in range(1, categories[category]):
            yield URLModel(disease=category, url=f'{BASE_URL}/page/{i}/')

def get_page_content(url: URLModel):
    def clean_page_content(content):
        return content.replace("\xa0", "")

    pattern = r'\b\d+\b'
    logger.info(f'url: {url.url}')
    html = requests.get(url.url).text
    soup = BeautifulSoup(html, 'html.parser')
    title = url.title
    logger.info(f'title: {title}')

    content_main = soup.find('div', id='ftwp-postcontent')
    logger.info(f'content_main: {content_main.text}')
    content=content_main.text
    content = clean_page_content(content)
    logger.info(f'content: {content}')
    
    metadata = "# url = " + url.url + "\n" + \
        "# date = " + url.date + "\n" + \
        title + "\n"
    content = metadata + content
    logger.info(f'content_main: {content}')
    path = join(CORPUS_LOCATION, url.disease)
    Path(path).mkdir(exist_ok=True, parents=True)
    article_id = random.randint(0, 1000000)
    with open(join(path , url.disease + f'_{article_id}.txt'), 'w+') as f:
        f.write(content)

def get_pages(pages: URLModel):
    logger.info(f'page_url: {pages.url}')
    html = requests.get(pages.url).text
    soup = BeautifulSoup(html, 'html.parser')

    urls = []
    first_article = soup.find('div', class_='col-sm-6 col-xs-12 item noleft')
    urls.append(
        URLModel
        (
            **{
                "disease": pages.disease,
                "url": first_article.find('a').get('href'),
                "date": first_article.find('div', class_='mb_10 mb_5mb small').get_text(strip=True),
                "title": first_article.find('a').get('title')
            }
        )
    )
    rows = soup.find_all('div', class_='row div_flex top')
    list_articles = []
    for row in rows:
        articles = row.find_all('div', class_='col-xs-12 col-sm-4 mb_15 item')
        list_articles.extend(articles)

    for article in list_articles:
        url = article.find('a').get('href')
        title = article.find('a', class_='title_post').get('title')
        date = article.find('div', class_='mb_10 mb_5mb small').get_text(strip=True)
        urls.append(
            URLModel
            (
                **{
                    "disease": pages.disease,
                    "url": url,
                    "date": date,
                    "title": title
                }
            )
        )
    logger.info(f'urls: {urls}')
    logger.info(f'len(urls): {len(urls)}')
    return urls

if __name__ == '__main__':
    page_urls = get_urls()
    threads = []
    for page_url in page_urls:
        logger.info(f'url: {page_url}')
        thread = ScrapeThread(page_url)
        thread.start()
        threads.append(thread)
        sleep(3)
        
    for thread in threads:
        thread.join()
    
    