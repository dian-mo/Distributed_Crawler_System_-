import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ====================== 导入模块 ======================
# DNS解析相关
from functools import lru_cache
from urllib.parse import urlparse
import socket

# 并发处理相关
import concurrent.futures

# 网络请求与解析
import re
import urllib.robotparser
import requests
import urllib.parse
from bs4 import BeautifulSoup

# 工具模块
import time
import random
import json
import hashlib
import certifi

# ====================== 全局配置 ======================
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'referer': 'https://www.suning.com/',
    'accept': 'text/html,application/xhtml+xml',
    'accept-encoding': 'gzip, deflate, br',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-User': '?1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache'
}

# ==================== DNS解析系统 ====================
class DNSCache:
    """DNS解析缓存系统，使用LRU策略缓存解析结果"""
    @lru_cache(maxsize=100)
    def resolve(self, domain):
        try:
            return socket.gethostbyname(domain)
        except socket.gaierror as e:
            print(f"DNS解析失败: {e}")
            return None

# ================== 随机IP代理系统 ===================
class ProxyManager:
    """代理IP管理系统，包含代理验证和维护功能"""
    def __init__(self):
        # 替换为有效代理列表
        self.proxies = [
            "http://58.60.255.104:8118",
        "http://219.135.164.245:3128",
        "http://27.44.171.27:9999",
        "http://219.135.164.245:3128",
        "http://58.60.255.104:8118",
        "http://58.252.6.165:9000"
        ]
        self.valid_proxies = []

    def verify_proxies(self):
        """多线程验证代理可用性"""
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(self._test_proxy, p): p for p in self.proxies}
            self.valid_proxies = [
                proxy for future, proxy in futures.items()
                if future.result()
            ]

    def _test_proxy(self, proxy):
        try:
            response = requests.get('https://ds.suning.com/ds/auth/checkInit',
                                    proxies={'http': proxy},
                                    timeout=5)
            # 改为验证标准HTTP状态码和响应时间
            return response.status_code == 200 and response.elapsed.total_seconds() < 2
        except:
            return False

# ==================== 系统初始化 ====================
dns_resolver = DNSCache()
pm = ProxyManager()
pm.verify_proxies()

# ==================== 爬虫系统 ======================
class ParallelCrawler:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0'
        ]
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch(self, url, page, cookies):
        headers = {
            'User-Agent': random.choice(self.user_agents),
            'Referer': 'https://search.suning.com/',
            'Sec-Fetch-Mode': 'navigate',
            # 新增苏宁专用请求头
            'Client': 'pc',
            'SN_DeviceId': hashlib.md5(str(time.time()).encode()).hexdigest()[:16],
            'Cookie': '; '.join([f'{k}={v}' for k, v in cookies.items()])  # 处理动态cookie
        }

        try:
            response = self.session.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            # 添加响应内容验证
            if not response.text.startswith('callback('):
                print(f"异常响应特征: 长度{len(response.text)} 状态码{response.status_code}")
            return response.text if response.status_code == 200 else None
        except Exception as e:
            print(f"请求失败: {str(e)}")
            return None

    def fetch_async(self, urls, cookies):
        """启动并行下载任务"""
        futures = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i, url in enumerate(urls, start=1):
                futures[executor.submit(self._fetch, url, i, cookies)] = url
        return futures

# ==================== 数据爬取流程 ===================
def fetch_suning_product(keyword, pages=3):
    # 初始化动态cookie（需从首次请求获取）
    init_response = requests.get('https://search.suning.com/', headers=HEADERS)
    dynamic_cookie = init_response.cookies.get_dict()

    # 使用固定搜索路径（保留原始URL参数）
    base_url = "https://search.suning.com/%E6%89%8B%E6%9C%BA/"

    # 更新请求参数模板
    params_template = {
        "safp": "d488778a.phone2018.gSearch.3",
        "safc": "hotkeyword.0.0",
        "safpn": "10003.00006",
        "sc": "0",
        "ct": "-1",
        "ci": "20032",
        "ps": "10",  # 保持原始分页参数
        "cp": "0"
    }

    # 新增cookie管理器
    cookies = dynamic_cookie.copy()
    cookies.update({
        'global_wapcity': '028',
        'global_cookie': hashlib.md5(str(time.time()).encode()).hexdigest()[:16]
    })

    urls = []
    for page in range(pages):
        params = params_template.copy()
        params.update({
            "cp": page,
            # 生成签名参数
            "_timestamp": int(time.time() * 1000),
            "uuid": hashlib.md5(f"{time.time()}".encode()).hexdigest()[:16]
        })

        # 调整签名算法格式
        sign_str = f'suning_phone_{params["uuid"]}_{params["_timestamp"]}_d488778a'
        params["sign"] = hashlib.md5(sign_str.encode()).hexdigest().lower()

        urls.append(f"{base_url}?{urllib.parse.urlencode(params)}")

    # 并行爬取
    crawler = ParallelCrawler()
    futures = crawler.fetch_async(urls, cookies)

    # 获取结果
    htmls = []
    for future in concurrent.futures.as_completed(futures):
        if (html := future.result()):
            htmls.append(html)

    # 新增：执行解析并返回结果
    return parallel_parse(htmls)

# ==================== 新增并行解析方法 ====================
def parallel_parse(htmls):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = []
        futures = {executor.submit(parse_suning_data, html): html for html in htmls}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.extend(result)
        return results

# ==================== 苏宁专用解析 ===================
def parse_suning_data(html):
    try:
        # 增强正则表达式容错性
        match = re.search(r'callback\(({.*})\);?', html, re.DOTALL)
        if not match:
            print("未找到有效JSONP响应")
            return []

        json_str = match.group(1)
        data = json.loads(json_str)

        # 添加字段存在性校验
        if 'goods' not in data or not isinstance(data['goods'], list):
            print("响应数据格式异常")
            return []

        return [{
            'id': item.get('commodityId', ''),
            'title': item.get('commodityName', ''),
            'price': item.get('price', 0),
            'url': f"https://product.suning.com/{item.get('supplierCode', '')}/{item.get('commodityId', '')}.html",
            'imgUrl': f"https:{item.get('commodityImage', '')}",
            'params': json.dumps({
                'sales': item.get('sales', 0),
                'shopId': item.get('supplierCode', ''),
                'rating': item.get('rating', 0)
            }, ensure_ascii=False)
        } for item in data['goods'] if item.get('commodityId')]
    except json.JSONDecodeError as e:
        print(f"JSON解析失败: {str(e)}")
        return []
    except Exception as e:
        print(f"解析错误: {str(e)}")
        return []

# ==================== 主程序入口 ====================
if __name__ == '__main__':
    try:
        products = fetch_suning_product("手机", 2)
        print(json.dumps(products[:2], indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"运行出错: {str(e)}")
