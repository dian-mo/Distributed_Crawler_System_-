import redis
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udf
import random
import time
import threading


# redis配置
REDIS_HOST = 'localhost' # 主机地址
REDIS_PORT = 6379 # Redis服务器端口 默认6379
REDIS_DB = 0 # Redis数据库编号

# 常量
SPIDER_WEBSITE_DOMAINS_KEY = 'spider.website.domains' # 爬虫网站域名集合的键名
SPIDER_SEED_URLS_KEY = 'spider.seed.urls' # 爬虫种子url列表的键名
SPIDER_DOMAINS_HIGHER_SUFFIX = '.higher' # 爬虫域名高优先级队列的后缀
SPIDER_DOMAINS_LOWER_SUFFIX = '.lower' # 爬虫域名低优先级队列的后缀


'''URL管理类'''
class URLManager:
    def __init__(self,redis_client):
        self.redis_client = redis_client
        # 检查键的数据类型
        key_type = redis_client.type(SPIDER_WEBSITE_DOMAINS_KEY)
        if key_type != 'set':
            print(f"键 spider.website.domains 的数据类型为 {key_type}，不是集合类型，将删除该键。")
            redis_client.delete(SPIDER_WEBSITE_DOMAINS_KEY)
            redis_client.sadd(SPIDER_WEBSITE_DOMAINS_KEY,None)
        # self.init_redis()

    # 初始化：清空Redis中高低优先队列
    '''
    暂时停用
    def init_redis(self):
        domains = self.redis_client.smembers(SPIDER_WEBSITE_DOMAINS_KEY)
        for domain in domains:
            # domain = domain.decode('utf-8')
            higher_key = domain + SPIDER_DOMAINS_HIGHER_SUFFIX
            lower_key = domain + SPIDER_DOMAINS_LOWER_SUFFIX
            self.redis_client.delete(higher_key,lower_key)
            self.redis_client.delete(SPIDER_WEBSITE_DOMAINS_KEY)
    '''

    # 添加种子URL
    def add_seed_urls(self,urls):
        for url in urls:
            self.redis_client.rpush(SPIDER_SEED_URLS_KEY,url) # 列表插入：rpush(键名，值) ；rpush插入列表尾部

    def poll_seed_urls(self):
        return self.redis_client.lpop(SPIDER_SEED_URLS_KEY)

    # 向高优先级队列添加URL
    def offer_higher(self,url):
        domain = self.get_top_domain(url)
        self.redis_client.sadd(SPIDER_WEBSITE_DOMAINS_KEY, domain)
        key = domain + SPIDER_DOMAINS_HIGHER_SUFFIX
        self.redis_client.lpush(key,url) # 列表插入 ；lpush插入列表首位

    # 向低优先级队列添加URL
    def offer_lower(self,url):
        domain = self.get_top_domain(url)
        self.redis_client.sadd(SPIDER_WEBSITE_DOMAINS_KEY, domain)
        key = domain + SPIDER_DOMAINS_LOWER_SUFFIX
        self.redis_client.lpush(key,url)

    # 获取URL的顶级域名
    def get_top_domain(self,url):
        from urllib.parse import urlparse
        parsed_url = urlparse(url) # 解析URL
        return parsed_url.netloc # 返回解析URL的域名部分

    # 从队列中获取URL：取域名——》组成高优先键 or 组成低优先键——》取URL
    def poll(self):
        domains = self.redis_client.smembers(SPIDER_WEBSITE_DOMAINS_KEY)
        if not domains:
            return None

        random_domain = random.choice(list(domains))
        higher_key = random_domain + SPIDER_DOMAINS_HIGHER_SUFFIX
        url = self.redis_client.rpop(higher_key)
        if url is None:
            lower_key =random_domain + SPIDER_DOMAINS_LOWER_SUFFIX
            url = self.redis_client.rpop(lower_key)
        if url is None:
            return None
        return url

'''URL分发策略'''
class URLDispatcher:
    def __init__(self,url_manager,redis_client):
        self.redis_client = redis_client
        self.url_manager = url_manager
        self.strategies = {
            'random': self.random_strategy,
            'priority': self.priority_strategy
        }

    # 随机分发策略
    def random_strategy(self,num_urls): # num_urls：待获取的URL数量
        urls = []
        for _ in range(num_urls):
            url = self.url_manager.poll()
            if url:
                urls.append(url)
        return urls

    # 优先级分发策略
    def priority_strategy(self,num_urls):
        urls = []
        for domain in self.redis_client.smembers(SPIDER_WEBSITE_DOMAINS_KEY):
            higher_key = domain + SPIDER_DOMAINS_HIGHER_SUFFIX
            while len(urls) < num_urls:
                url = self.redis_client.rpop(higher_key)
                if url:
                    urls.append(url)
                else:
                    break
        while len(urls) < num_urls: # 第一部分按顺序取高级优先队列，Now:第二部分调用函数随机取低级优先队列
            url = self.url_manager.poll()
            if url:
                urls.append(url)
        return urls

    # 按策略分发URL
    def dispatch(self,strategy_name,num_urls):
        strategy = self.strategies.get(strategy_name)
        if strategy:
            return strategy(num_urls)
        return []

def main():
    # 创建依赖对象
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0,
                                decode_responses=True)  # host为主机的IP，端口为6379，数据库为默认，字符串格式
    redis_client = redis.Redis(connection_pool=pool)

    # 注入依赖到相关类
    url_manager = URLManager(redis_client)

    # 初始化分发器
    dispatcher = URLDispatcher(url_manager,redis_client)

    # 种子URL输入Redis
    seed_urls = ["https://search.jd.com/Search?keyword=%E6%89%8B%E6%9C%BA&enc=utf-8"]
    url_manager.add_seed_urls(seed_urls)

    # 爬虫爬取种子URL信息
    print(url_manager.poll_seed_urls())

    # 爬虫获取待爬取URL，并输入Redis
    new_urls = [
        "https://search.jd.com/Search?keyword=%E6%89%8B%E6%9C%BA&enc=utf-8&page=3",  # 不同页码
        "https://item.jd.com/100044835937.html",
        "https://item.jd.com/100149709050.html"
    ]
    for url in new_urls:
        url_manager.offer_higher(url)
        # url_manager.offer_lower(url)

    # 分发任务
    crawler_ids = [1, 2, 3]
    for crawler_id in crawler_ids:
        urls = dispatcher.dispatch('random', 2)
        print(urls)


if __name__ == "__main__":
    main()