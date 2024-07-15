from os.path import dirname, join, exists
from pathlib import Path
import re
from loguru import logger
import requests
from bs4 import BeautifulSoup



CORPUS_LOCATION = join(dirname(dirname(__file__)), "datasets", "crawl")

def get_urls():
    categories = [
        "suc-khoe/song-khoe",
        "suc-khoe/ung-thu",
        "suc-khoe/dich-vu-y-te-quoc-te",
        "suc-khoe/kien-thuc-gioi-tinh",
        "suc-khoe/tu-van",
        "suc-khoe/khoe-dep",
        "suc-khoe/suc-khoe-chu-dong"
    ]
    for page in range(1, 30):
        for category in categories:
            yield f"https://dantri.com.vn/{category}/trang-{page}.htm"

def article_already_crawled(article_id, category):
    path = join(CORPUS_LOCATION, category, f'{article_id}.txt')
    return exists(path)

def get_page_content(article_url, category):
    def clean_page_content(content):
        return content.replace("\xa0", "")
        
    article_id = article_url.split('/')[-1]
    if article_already_crawled(article_id, category):
        logger.warning(f"Article {article_url} already crawled")
        return

    try:
        logger.info(f"Crawl article {article_url}")
        html = requests.get(url=article_url).text

        pattern = r'\b\d+\b'
        article_id = re.search(pattern, article_id).group(0)
        logger.info(f'article_id {article_id}')
        date = article_id.split('-')[-1].split('.')[0][:8]
        logger.info(f'date: {date}')
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.title.text.split('|')[0]
        logger.info(f'Title: {title}')

        singular_content_div = soup.find('div', {'class': 'singular-content'})
        if singular_content_div:
            paragraphs = singular_content_div.find_all('p')
            content = '\n'.join(p.text for p in paragraphs)
            content = clean_page_content(content)
        else:
            logger.warning(f'No singular-content div found for article {article_url}')
            content = soup.find('div', {'class': 'e-magazine__body'}).text

        metadata = "# url = " + article_url + "\n" + \
            "# date = " + date + "\n" + \
            title + "\n"
        content = metadata + content
        logger.info(f'content: {content}')
        path = join(CORPUS_LOCATION, category)
        Path(path).mkdir(exist_ok=True, parents=True)
        with open(join(path ,article_id + '.txt'), 'w+') as f:
            f.write(content)
    except ValueError:
        raise ValueError(f"Error when crawl article {article_url}")

def get_page(page_url):
    logger.info(f'page_url: {page_url}')
    category = page_url.split('/')[-2]
    logger.info(f'category: {category}')
    html = requests.get(page_url).text
    soup = BeautifulSoup(html, 'html.parser')
    article_items = soup.find_all('article', {'class': 'article-item'})
    hrefs = [item.find('a').get('href') for item in article_items]
    urls = ["https://dantri.com.vn" + href for href in hrefs]
    for url in urls:
        get_page_content(url, category)


if __name__ == '__main__':
    urls = get_urls()
    for url in urls:
        logger.info(f"url: {url}")
        get_page(url)