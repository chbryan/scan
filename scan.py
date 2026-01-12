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
from scrapy_redis.dupefilter import RFPDupeFilter
from kafka import KafkaProducer
import redis
import pika
import json
from urllib.parse import urlparse
from scrapy.utils.misc import load_object
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

logging.basicConfig(filename='site_log.txt', level=logging.INFO, format='%(message)s')

class CustomRFPDupeFilter(RFPDupeFilter):
    def __init__(self, *args, **kwargs):
        super(CustomRFPDupeFilter, self).__init__(*args, **kwargs)
        logging.info("Custom DupeFilter initialized")

class RabbitMQScheduler(object):
    def __init__(self, dupefilter):
        self.dupefilter = dupefilter
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='url_queue', durable=True)
        self.spider = None

    @classmethod
    def from_crawler(cls, crawler):
        dupefilter = load_object(crawler.settings['DUPEFILTER_CLASS']).from_crawler(crawler)
        return cls(dupefilter)

    def open(self, spider):
        self.spider = spider
        self.dupefilter.open()

    def close(self, reason):
        self.dupefilter.close(reason)
        self.connection.close()

    def enqueue_request(self, request):
        if not request.dont_filter and self.dupefilter.request_seen(request):
            return False
        self.channel.basic_publish(exchange='', routing_key='url_queue', body=request.url, properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
        return True

    def next_request(self):
        method_frame, _, body = self.channel.basic_get('url_queue')
        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
            return self.spider.make_request_from_url(body.decode())
        return None

    def __len__(self):
        result = self.channel.queue_declare(queue='url_queue', passive=True)
        return result.method.message_count

    def has_pending_requests(self):
        return len(self) > 0

class PageItem(Item):
    url = Field()
    title = Field()
    full_html = Field()  # Changed to full HTML for depth
    meta_description = Field()
    keywords = Field()
    h1 = Field()
    headers = Field()  # All headers
    paragraphs = Field()
    images = Field()
    links = Field()
    scripts = Field()
    canonical = Field()
    og_title = Field()
    og_description = Field()
    og_image = Field()
    meta_tags = Field()  # All meta
    timestamp = Field()

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
        logging.info(f"URL: {item['url']}\nTimestamp: {item['timestamp']}\nTitle: {item.get('title', '')}\nMeta Description: {item.get('meta_description', '')}\nKeywords: {item.get('keywords', '')}\nH1: {item.get('h1', '')}\nHeaders: {item.get('headers', [])}\nParagraphs: {item.get('paragraphs', [])}\nImages: {item.get('images', [])}\nLinks: {item.get('links', [])}\nScripts: {item.get('scripts', [])}\nCanonical: {item.get('canonical', '')}\nOG Title: {item.get('og_title', '')}\nOG Description: {item.get('og_description', '')}\nOG Image: {item.get('og_image', '')}\nMeta Tags: {item.get('meta_tags', {})}\nFull HTML: {item['full_html']}\n---")
        return item

class PostgresPipeline(object):
    def __init__(self):
        self.conn = psycopg2.connect(dbname='your_db', user='your_user', password='your_pass', host='localhost')
        self.cur = self.conn.cursor()
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                title TEXT,
                full_html TEXT,
                meta_description TEXT,
                keywords TEXT,
                h1 TEXT,
                headers TEXT[],
                paragraphs TEXT[],
                images TEXT[],
                links TEXT[],
                scripts TEXT[],
                canonical TEXT,
                og_title TEXT,
                og_description TEXT,
                og_image TEXT,
                meta_tags JSONB,
                timestamp TIMESTAMP
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        try:
            self.cur.execute(sql.SQL("""
                INSERT INTO pages (url, title, full_html, meta_description, keywords, h1, headers, paragraphs, images, links, scripts, canonical, og_title, og_description, og_image, meta_tags, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                full_html = EXCLUDED.full_html,
                meta_description = EXCLUDED.meta_description,
                keywords = EXCLUDED.keywords,
                h1 = EXCLUDED.h1,
                headers = EXCLUDED.headers,
                paragraphs = EXCLUDED.paragraphs,
                images = EXCLUDED.images,
                links = EXCLUDED.links,
                scripts = EXCLUDED.scripts,
                canonical = EXCLUDED.canonical,
                og_title = EXCLUDED.og_title,
                og_description = EXCLUDED.og_description,
                og_image = EXCLUDED.og_image,
                meta_tags = EXCLUDED.meta_tags,
                timestamp = EXCLUDED.timestamp
            """), (item['url'], item.get('title'), item['full_html'], item.get('meta_description'), item.get('keywords'), item.get('h1'), item.get('headers'), item.get('paragraphs'), item.get('images'), item.get('links'), item.get('scripts'), item.get('canonical'), item.get('og_title'), item.get('og_description'), item.get('og_image'), json.dumps(item.get('meta_tags', {})), item['timestamp']))
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

class KafkaPipeline(object):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def process_item(self, item, spider):
        try:
            self.producer.send('site_topic', value=dict(item))
        except Exception as e:
            logging.error(f"Kafka Error: {e}")
        return item

class RabbitMQPipeline(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='site_queue', durable=True)

    def process_item(self, item, spider):
        try:
            self.channel.basic_publish(exchange='', routing_key='site_queue', body=json.dumps(dict(item)))
        except Exception as e:
            logging.error(f"RabbitMQ Error: {e}")
        return item

    def close_spider(self, spider):
        self.connection.close()

class SiteSpider(scrapy.Spider):
    name = 'site_spider'
    link_extractor = LinkExtractor()
    
    def __init__(self, domain=None, *args, **kwargs):
        super(SiteSpider, self).__init__(*args, **kwargs)
        self.allowed_domains = [domain]
    
    def start_requests(self):
        return []
    
    def make_request_from_url(self, url):
        return SeleniumRequest(url, self.parse, wait_time=2, errback=self.errback)
    
    def parse(self, response):
        try:
            item = PageItem()
            item['url'] = response.url
            item['timestamp'] = datetime.now().isoformat()
            item['title'] = response.css('title::text').get()
            item['full_html'] = response.text  # Full source
            item['meta_description'] = response.css('meta[name="description"]::attr(content)').get()
            item['keywords'] = response.css('meta[name="keywords"]::attr(content)').get()
            item['h1'] = response.css('h1::text').get()
            item['headers'] = response.css('h1,h2,h3,h4,h5,h6::text').getall()
            item['paragraphs'] = response.css('p::text').getall()
            item['images'] = [response.urljoin(src) for src in response.css('img::attr(src)').getall()]
            item['links'] = [response.urljoin(link.url) for link in self.link_extractor.extract_links(response)]
            item['scripts'] = response.css('script::attr(src)').getall() + response.css('script::text').getall()
            item['canonical'] = response.css('link[rel="canonical"]::attr(href)').get()
            item['og_title'] = response.css('meta[property="og:title"]::attr(content)').get()
            item['og_description'] = response.css('meta[property="og:description"]::attr(content)').get()
            item['og_image'] = response.css('meta[property="og:image"]::attr(content)').get()
            item['meta_tags'] = {response.xpath('@name').get(): response.xpath('@content').get() for response in response.css('meta[name]')}
            yield item
            
            for link in self.link_extractor.extract_links(response):
                yield SeleniumRequest(link.url, self.parse, wait_time=2, errback=self.errback)
        except Exception as e:
            logging.error(f"Parse Error: {e}")

    def errback(self, failure):
        logging.error(f"Request Failed: {failure.request.url} - {failure.value}")

process = CrawlerProcess(settings={
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'DOWNLOAD_DELAY': random.uniform(1, 5),
    'DEPTH_LIMIT': 0,  # Unlimited depth
    'ROBOTSTXT_OBEY': True,  # Respect robots.txt
    'DUPEFILTER_CLASS': '__main__.CustomRFPDupeFilter',
    'SCHEDULER': '__main__.RabbitMQScheduler',
    'REDIS_URL': 'redis://localhost:6379',
    'DOWNLOADER_MIDDLEWARES': {
        'scrapy_selenium.SeleniumMiddleware': 800,
    },
    'ITEM_PIPELINES': {
        '__main__.JsonPipeline': 300,
        '__main__.TxtPipeline': 400,
        '__main__.PostgresPipeline': 500,
        '__main__.ElasticsearchPipeline': 600,
        '__main__.KafkaPipeline': 700,
        '__main__.RabbitMQPipeline': 800,
    },
    'SELENIUM_DRIVER_NAME': 'chrome',
    'SELENIUM_DRIVER_EXECUTABLE_PATH': which('chromedriver'),
    'SELENIUM_DRIVER_ARGUMENTS': ['--headless'],
})

site_url = input("Enter site address: ")
domain = urlparse(site_url).netloc

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='url_queue', durable=True)
channel.basic_publish(exchange='', routing_key='url_queue', body=site_url, properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
connection.close()

process.crawl(SiteSpider, domain=domain)
process.start()
