from os.path import dirname, join, exists
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
            get_page_content(url.url, url.disease)
        



CORPUS_LOCATION = join(dirname(dirname(__file__)), "datasets", "crawl")

class URLModel(BaseModel):
    disease: str
    url: str

BASE_URL = 'https://tamanhhospital.vn/'

def get_urls():
    categories = {
        'benh': 'disease'
    }
    for i in range(1,430):
        for category in categories.keys():
            yield URLModel(disease=categories[category], url=f'https://tamanhhospital.vn/{category}/page/{i}')

def get_page_content(aritcle_url, category):
    def clean_page_content(content):
        return content.replace("\xa0", "")

    pattern = r'\b\d+\b'
    html = requests.get(aritcle_url).text
    soup = BeautifulSoup(html, 'html.parser')
    date = soup.find('div', class_='mb_10 small div_tool')
    date = date.get_text(strip=True)
    logger.info(f'date: {date}')
    date_pattern = r'\d{2}/\d{2}/\d{4}'
    date = re.search(date_pattern, date).group()
    logger.info(f'date: {date}')

    title = soup.title.text
    logger.info(f'title: {title}')

    content_main = soup.find('div', id='ftwp-postcontent')

    arlert_info = content_main.find('div', class_='alert alert-info')
    if arlert_info:
        arlert_info.decompose()
    
    content=content_main.get_text(separator='')
    content = clean_page_content(content)
    
    metadata = "# url = " + aritcle_url + "\n" + \
        "# date = " + date + "\n" + \
        title + "\n"
    content = metadata + content
    logger.info(f'content_main: {content}')
    path = join(CORPUS_LOCATION, 'disease')
    Path(path).mkdir(exist_ok=True, parents=True)
    article_id = random.randint(0, 1000000)
    with open(join(path , category + f'_{article_id}.txt'), 'w+') as f:
        f.write(content)

def get_pages(pages: URLModel):
    logger.info(f'page_url: {pages.url}')
    html = requests.get(pages.url).text
    soup = BeautifulSoup(html, 'html.parser')

    rows = soup.find_all('div', class_='row div_flex top')
    logger.info(len(rows))
    urls = []
    for row in rows:
        a_tag = row.find_all('a', class_='thumb_post col-sm-12 col-xs-6 item')
        urls.extend([a.get('href') for a in a_tag])
    urls = [
        URLModel(
            **{
                "disease": pages.disease,
                "url": url
            }
        ) for url in urls
    ]
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
        sleep(1)
        
    for thread in threads:
        thread.join()
    # test get_pages_content
    # url = 'https://tamanhhospital.vn/bi-viem-phu-khoa-nen-an-gi/'
    # get_page_content(url, 'disease')