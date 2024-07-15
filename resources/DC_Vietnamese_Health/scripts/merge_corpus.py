from loguru import logger
from os import listdir
from os.path import join, dirname
from pathlib import Path

ROOT_FOLDER = dirname(dirname(__file__))

CRAWL_FOLDER = join(ROOT_FOLDER, "datasets", "crawl")
DATASETS_FOLDER = join(ROOT_FOLDER, "corpus", "DC_Vietnamese_Heatlh")

CATEGORIES = {}
CATEGORIES_MAP = {
    'song-khoe': 'healthy-living',
    'ung-thu': 'cancer',
    'dich-vu-y-te-quoc-te': 'international-health-services',
    'kien-thuc-gioi-tinh': 'sexual-education',
    'tu-van': 'consultation',
    'khoe-dep': 'beauty-wellness',
    'suc-khoe-chu-dong': 'proactive-health'
}


DOC_URLS = set()

def fetch_current_dataset():
    pass

def update_dataset(crawl_folder):
    global CATEGORIES
    global DOC_URLS
    categories_folder = sorted(listdir(crawl_folder))
    for category_folder in categories_folder:
        list_files = listdir(join(crawl_folder, category_folder))
        for file in list_files:
            with open(join(crawl_folder, category_folder, file)) as f:
                content = f.read()
                lines = content.splitlines()
                lines = [_.strip() for _ in lines]
                lines = [_ for _ in lines if len(_) > 0]
                content = "\n".join(lines)
                url = lines[0][8:]
                logger.info(f"content: {content}")
                logger.info(f"url: {url}")
                if len(lines) <= 5:
                    print(f"File {file} has less than 3 lines ({url})")
                    continue
                if url in DOC_URLS:
                    print(f"URL is existed. ({url})")
                    continue
                DOC_URLS.add(url)
                genre = "health"
                category_text_id = category_folder
                logger.info(f"category_text_id: {category_text_id}")
                if category_text_id not in CATEGORIES_MAP:
                    continue
                category = CATEGORIES_MAP[category_text_id]
                if category not in CATEGORIES:
                    CATEGORIES[category] = 0
                CATEGORIES[category] += 1 
                doc_id_number = CATEGORIES[category]
                doc_id_number_text = f"{doc_id_number:03d}"
                doc_id = "_".join([genre, category, doc_id_number_text])
                doc_id_line = f"# doc_id = {doc_id}\n"
                doc_filename = f"{doc_id}.txt"
                doc_filepath = join(DATASETS_FOLDER,doc_filename)
                with open(doc_filepath, "w") as f:
                    output = doc_id_line + content
                    f.write(output)

if __name__ == "__main__":
    Path(DATASETS_FOLDER).mkdir(exist_ok=True, parents=True)
    update_dataset(CRAWL_FOLDER)
