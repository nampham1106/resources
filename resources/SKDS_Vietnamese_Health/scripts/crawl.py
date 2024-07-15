from os.path import dirname, join, exists
from time import sleep
from tqdm import tqdm
from pathlib import Path
from pydantic import BaseModel
import re
import threading
from loguru import logger
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

options = Options()
options.add_argument('--disable-blink-features=AutomationControlled')
options.log.level = "trace"


class ScrapeThread(threading.Thread):
    def __init__(self, disease_page):
        threading.Thread.__init__(self)
        self.disease_page = disease_page
    
    def run(self):
        driver = webdriver.Firefox(options=options)
        driver.get(self.disease_page.url)
        page_source = driver.page_source
        driver.close()
        soup = BeautifulSoup(page_source, 'html.parser')
        list_url = soup.find('div', {'class': 'list__tcb'}).find_all('h3')
        list_url = [BASE_URL + item.find('a').get('href') for item in list_url]
        for url in list_url:
            get_page_content(url, self.disease_page.disease)
        logger.info(f'list_url: {list_url}')



CORPUS_LOCATION = join(dirname(dirname(__file__)), "datasets", "crawl")

class URLModel(BaseModel):
    disease: str
    url: str

BASE_URL = 'https://suckhoedoisong.vn'

def get_disease_list():
    diseases = []
    brower = webdriver.Firefox(options=options)
    url = 'https://suckhoedoisong.vn/tra-cuu-benh.htm'
    brower.get(url)
    html_source = brower.page_source
    brower.close()
    soup = BeautifulSoup(html_source, 'html.parser')
    box_items = soup.find_all('div', {'class': 'box-item'})
    # find all url in box_items
    for item in box_items:
        a = item.find_all('a')
        list_url = [
            URLModel(
                **{
                    "disease": item.get_text(),
                    "url": BASE_URL + item.get('href')
                }
            ) for item in a
        ]
        diseases.extend(list_url)
    return diseases

def get_page_content(aritcle_url, category):
    def clean_page_content(content):
        return content.replace("\xa0", "")

    pattern = r'\b\d+\b'
    article_id = re.search(pattern, aritcle_url).group(0)
    html = requests.get(aritcle_url).text
    soup = BeautifulSoup(html, 'html.parser')
    date = soup.find('span', class_='publish-date')
    date = date.get_text(strip=True).split()[0]
    logger.info(f'date: {date}')

    title = soup.title.text
    logger.info(f'title: {title}')

    content_main = soup.find('div', itemprop='articleBody')
    toc_title = soup.find('div', class_='toc-list-headings VCSortableInPreviewMode alignRight')
    if toc_title:
        toc_title.decompose()
    
    content=content_main.get_text(separator='\n')
    content = clean_page_content(content)
    
    metadata = "# url = " + aritcle_url + "\n" + \
        "# date = " + date + "\n" + \
        title + "\n"
    content = metadata + content
    logger.info(f'content_main: {content}')
    path = join(CORPUS_LOCATION, 'disease')
    Path(path).mkdir(exist_ok=True, parents=True)
    with open(join(path , category + f'_{article_id}.txt'), 'w+') as f:
        f.write(content)

# def get_page(disease_page):
#     logger.info(f'Get page {disease_page.url}')
#     brower = webdriver.Firefox(options=options)
#     brower.get(disease_page.url)
#     html_source = brower.page_source
#     brower.quit()
    


if __name__ == '__main__':
    disease_list = get_disease_list()
    threads = []
    for url in tqdm(disease_list, desc='Crawling'):
        thread = ScrapeThread(url)
        thread.start()
        threads.append(thread)
        sleep(1)
    
    for thread in threads:
        thread.join()
    