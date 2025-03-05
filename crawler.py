import sys
import io
import re
import time
import random
import json
import hashlib
import concurrent.futures
from urllib.parse import urljoin, urlencode
from queue import Queue
from bs4 import BeautifulSoup
import requests
import happybase
import logging
from abc import ABC, abstractmethod

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# HBase 配置
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
HBASE_TABLE = 'suning_products'
COLUMN_FAMILY = 'info'

# 初始化 HBase 连接
try:
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    connection.open()
    logger.info("HBase 连接成功")
except Exception as e:
    logger.error(f"HBase 连接失败: {e}")
    sys.exit(1)

# 创建 HBase 表（如果不存在）
try:
    if HBASE_TABLE.encode() not in connection.tables():
        connection.create_table(HBASE_TABLE, {COLUMN_FAMILY: dict()})
        logger.info(f"HBase 表 {HBASE_TABLE} 创建成功")
except Exception as e:
    logger.error(f"HBase 表 {HBASE_TABLE} 创建失败: {e}")

# 全局配置
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Referer': 'https://search.suning.com/',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
}

# 代理管理模块（单例模式）
class ProxyRepository:
    _instance = None
    proxies = [
        "http://58.60.255.104:8118",
        "http://219.135.164.245:3128",
        "http://27.44.171.27:9999",
    ]

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_random_proxy(self):
        """随机获取代理"""
        if self.proxies:
            return random.choice(self.proxies)
        return None

# 下载器接口
class IDownload(ABC):
    @abstractmethod
    def download(self, url: str) -> str:
        pass

# HTTP下载器实现
class HttpGetDownloadImpl(IDownload):
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def download(self, url: str) -> str:
        try:
            response = self.session.get(url, proxies=None, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"下载失败: {str(e)}")
            return ""

# 解析器接口
class IParser(ABC):
    @abstractmethod
    def parse(self, html: str) -> dict:
        pass

# 苏宁解析器实现
class SuningParserImpl(IParser):
    def parse(self, html: str) -> dict:
        try:
            if not html.strip():
                return {'status': 'error', 'reason': '空HTML内容'}

            soup = BeautifulSoup(html, 'lxml')
            product_list = []

            items = soup.select('.product-item') or []

            for item in items:
                title_elem = item.select_one('.title')
                price_elem = item.select_one('.price')
                if title_elem and price_elem:
                    product_list.append({
                        'title': title_elem.text.strip(),
                        'price': price_elem.text.strip()
                    })

            if not product_list:
                return {'status': 'empty'}
            return {'status': 'success', 'data': product_list}
        except Exception as e:
            logger.error(f"解析失败: {str(e)}")
            return {'status': 'error', 'reason': str(e)}

# 核心爬虫类
class SuningCrawler:
    def __init__(self, downloader: IDownload, parser: IParser):
        self.downloader = downloader
        self.parser = parser
        self.request_queue = Queue(maxsize=1000)
        self._start_workers()

    def _start_workers(self):
        import threading
        num_threads = 5  # 增加线程数量
        logger.info(f"启动 {num_threads} 个工作线程")
        for _ in range(num_threads):
            threading.Thread(target=self._worker, daemon=True).start()

def _worker(self):
    """工作线程逻辑"""
    while True:
        url = self.request_queue.get()
        try:
            logger.info(f"开始爬取 URL: {url}")
            html = self.downloader.download(url)
            if html:
                data = self.parser.parse(html)
                if data['status'] == 'success':
                    logger.info(f"从 {url} 爬取到的商品信息：")
                    table = connection.table(HBASE_TABLE)
                    for product in data['data']:
                        logger.info(f"标题: {product['title']}, 价格: {product['price']}")
                        row_key = hashlib.md5(f"{product['title']}{product['price']}".encode()).hexdigest()
                        table.put(row_key, {
                            f"{COLUMN_FAMILY}:title": product['title'],
                            f"{COLUMN_FAMILY}:price": product['price']
                        })
                        logger.info(f"数据已存储到 HBase: {row_key}")
                elif data['status'] == 'empty':
                    logger.warning(f"页面 {url} 没有商品数据")
                else:
                    logger.error(f"解析失败: {data['reason']}")
        except Exception as e:
            logger.error(f"处理 {url} 时出错: {str(e)}")
        finally:
            self.request_queue.task_done()