```python
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy_selenium import SeleniumRequest
from scrapy.exporters import JsonItemExporter
from scrapy.item import Item, Field
from shutil import which
import random
import logging
import psycopg2
from psycopg2 import sql
from elasticsearch import Elasticsearch

logging.basicConfig(filename='site_log.txt', level=logging.INFO, format='%(message)s')

class PageItem(Item):
    url = Field()
    title = Field()
    content = Field()
    meta_description = Field()
    keywords = Field()
    h1 = Field()
    images = Field()
    links = Field()

class JsonPipeline(object):
    def open_spider(self, spider):
        self.file = open('site_data.json', 'w')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

class TxtPipeline(object):
    def process_item(self, item, spider):
        logging.info(f"URL: {item['url']}\nTitle: {item.get('title', '')}\nMeta Description: {item.get('meta_description', '')}\nKeywords: {item.get('keywords', '')}\nH1: {item.get('h1', '')}\nImages: {item.get('images', [])}\nLinks: {item.get('links', [])}\nContent: {item['content']}\n---")
        return item

class PostgresPipeline(object):
    def __init__(self):
        self.conn = psycopg2.connect(dbname='your_db', user='your_user', password='your_pass', host='localhost')
        self.cur = self.conn.cursor()
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                title TEXT,
                meta_description TEXT,
                keywords TEXT,
                h1 TEXT,
                images TEXT[],
                links TEXT[],
                content TEXT
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        try:
            self.cur.execute(sql.SQL("""
                INSERT INTO pages (url, title, meta_description, keywords, h1, images, links, content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                meta_description = EXCLUDED.meta_description,
                keywords = EXCLUDED.keywords,
                h1 = EXCLUDED.h1,
                images = EXCLUDED.images,
                links = EXCLUDED.links,
                content = EXCLUDED.content
            """), (item['url'], item.get('title'), item.get('meta_description'), item.get('keywords'), item.get('h1'), item.get('images'), item.get('links'), item['content']))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"DB Error: {e}")
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()

class ElasticsearchPipeline(object):
    def __init__(self):
        self.es = Elasticsearch(['localhost:9200'])

    def process_item(self, item, spider):
        try:
            self.es.index(index='site_index', id=item['url'], body=dict(item))
        except Exception as e:
            logging.error(f"ES Error: {e}")
        return item

class SiteSpider(scrapy.Spider):
    name = 'site_spider'
    
    def __init__(self, start_url=None, *args, **kwargs):
        super(SiteSpider, self).__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.allowed_domains = [start_url.split('//')[-1].split('/')[0]]
    
    def start_requests(self):
        for url in self.start_urls:
            yield SeleniumRequest(url, self.parse, wait_time=2, errback=self.errback)
    
    def parse(self, response):
        try:
            item = PageItem()
            item['url'] = response.url
            item['title'] = response.css('title::text').get()
            item['meta_description'] = response.css('meta[name="description"]::attr(content)').get()
            item['keywords'] = response.css('meta[name="keywords"]::attr(content)').get()
            item['h1'] = response.css('h1::text').get()
            item['images'] = response.css('img::attr(src)').getall()
            item['links'] = response.css('a::attr(href)').getall()
            item['content'] = ' '.join(response.css('body ::text').getall()).strip()
            yield item
            
            for href in item['links']:
                yield SeleniumRequest(response.urljoin(href), self.parse, wait_time=2, errback=self.errback)
        except Exception as e:
            logging.error(f"Parse Error: {e}")

    def errback(self, failure):
        logging.error(f"Request Failed: {failure.request.url} - {failure.value}")

process = CrawlerProcess(settings={
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'DOWNLOAD_DELAY': random.uniform(1, 5),
    'DEPTH_LIMIT': 5,
    'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter',  # Explicit duplicate URL filtering
    'DOWNLOADER_MIDDLEWARES': {
        'scrapy_selenium.SeleniumMiddleware': 800,
    },
    'ITEM_PIPELINES': {
        '__main__.JsonPipeline': 300,
        '__main__.TxtPipeline': 400,
        '__main__.PostgresPipeline': 500,
        '__main__.ElasticsearchPipeline': 600,
    },
    'SELENIUM_DRIVER_NAME': 'chrome',
    'SELENIUM_DRIVER_EXECUTABLE_PATH': which('chromedriver'),
    'SELENIUM_DRIVER_ARGUMENTS': ['--headless'],
})

site_url = input("Enter site address: ")
process.crawl(SiteSpider, start_url=site_url)
process.start()
```
