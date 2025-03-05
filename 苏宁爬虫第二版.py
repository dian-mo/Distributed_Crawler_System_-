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

# 解决控制台编码问题
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ====================== 全局配置 ======================
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Referer': 'https://search.suning.com/',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
}

# ====================== 代理管理模块 ======================
class ProxyRepository:
    """代理仓库（单例模式）"""
    _instance = None
    proxies = [
        "http://58.60.255.104:8118",
        "http://219.135.164.245:3128",
        "http://27.44.171.27:9999",
        "http://219.135.164.245:3128",
        "http://58.60.255.104:8118",
        "http://58.252.6.165:9000"
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

# ====================== 接口定义 ======================
from abc import ABC, abstractmethod

class IDownload(ABC):
    """下载器接口"""
    @abstractmethod
    def download(self, url: str) -> str:
        pass

class IParser(ABC):
    """解析器接口"""
    @abstractmethod
    def parse(self, html: str) -> dict:
        pass

# ====================== 实现类 ======================
class HttpGetDownloadImpl(IDownload):
    """HTTP下载器实现"""
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def download(self, url: str) -> str:
        """无代理的下载实现"""
        try:
            response = self.session.get(
                url,
                proxies=None,
                timeout=10
            )
            response.raise_for_status()  # 检查响应状态码
            return response.text
        except requests.RequestException as e:
            print(f"下载失败: {str(e)}")
            return ""

# ====================== 实现类 ======================
class SuningParserImpl(IParser):
    """苏宁解析器实现"""
    def parse(self, html: str) -> dict:
        try:
            if not html.strip():
                return {'status': 'error', 'reason': '空HTML内容'}

            soup = BeautifulSoup(html, 'lxml')
            product_list = []

            # 修改选择器
            items = soup.select('.product-item') or []

            for item in items:
                title_elem = item.select_one('.title')
                price_elem = item.select_one('.price')
                if title_elem and price_elem:
                    product_list.append({
                        'title': title_elem.text.strip(),
                        'price': price_elem.text.strip()
                    })

            return {
                'status': 'success' if product_list else 'empty',
                'data': product_list
            }
        except Exception as e:
            print(f"解析失败: {str(e)}")
            return {'status': 'error'}

# ====================== 核心爬虫类改造 ======================
class SuningCrawler:
    def __init__(self, downloader: IDownload, parser: IParser):
        self.downloader = downloader
        self.parser = parser
        self.request_queue = Queue(maxsize=1000)
        self._start_workers()

    def _start_workers(self):
        """启动工作线程池"""
        import threading
        for _ in range(3):  # 3个工作线程
            threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        """改造后的工作线程"""
        while True:
            url = self.request_queue.get()
            try:
                html = self.downloader.download(url)
                if html:
                    data = self.parser.parse(html)
                    if data['status'] == 'success':
                        print(f"从 {url} 爬取到的商品信息：")
                        for product in data['data']:
                            print(f"标题: {product['title']}, 价格: {product['price']}")
            finally:
                self.request_queue.task_done()

    # 新增任务提交方法
    def submit_task(self, url):
        """添加请求到队列"""
        self.request_queue.put(url)

# ====================== URL生成工具 ======================
def generate_search_urls(base_url: str, pages=3) -> list:
    """生成苏宁搜索分页URL"""
    urls = []
    for page in range(pages):
        params = {
            "safp": "d488778a.phone2018.gSearch.3",
            "cp": page,
            "_timestamp": int(time.time() * 1000),
            "uuid": hashlib.md5(f"{time.time()}".encode()).hexdigest()[:16]
        }
        # 生成签名参数
        sign_str = f'suning_phone_{params["uuid"]}_{params["_timestamp"]}_d488778a'
        params["sign"] = hashlib.md5(sign_str.encode()).hexdigest().lower()
        urls.append(f"{base_url}?{urlencode(params)}")
    return urls

# ====================== 测试案例 ======================
import unittest

class TestSuningParser(unittest.TestCase):
    """苏宁解析器测试用例"""

    def setUp(self):
        self.parser = SuningParserImpl()

    def test_normal_case(self):
        """正常商品列表解析测试"""
        mock_html = """
        <div class="product-item">
            <div class="title">华为Mate60 Pro</div>
            <div class="price">￥6999</div>
        </div>
        <div class="product-item">
            <div class="title">iPhone 15</div>
            <div class="price">￥5999</div>
        </div>
        """
        result = self.parser.parse(mock_html)
        self.assertEqual(result['status'], 'success')
        self.assertEqual(len(result['data']), 2)
        self.assertIn('华为', result['data'][0]['title'])

    def test_empty_case(self):
        """空页面解析测试"""
        result = self.parser.parse("")
        self.assertEqual(result['status'], 'error')

    def test_empty_data_case(self):
        """无商品数据测试"""
        mock_html = """
        <div class="product-container">
            <div class="no-data">暂时没有商品</div>
        </div>
        """
        result = self.parser.parse(mock_html)
        self.assertEqual(result['status'], 'empty')

if __name__ == '__main__':
    # 执行测试用例
    unittest.main(argv=[''], exit=False)

    # 原有执行流程
    # 初始化组件
    downloader = HttpGetDownloadImpl()
    parser = SuningParserImpl()

    # 创建爬虫实例
    crawler = SuningCrawler(downloader, parser)

    # 创建爬虫实例后添加任务
    base_url = "https://search.suning.com/%E6%89%8B%E6%9C%BA/"
    search_urls = generate_search_urls(base_url, pages=2)
    for url in search_urls:
        crawler.submit_task(url)

    # 等待队列处理完成
    crawler.request_queue.join()
    print("数据采集完成")