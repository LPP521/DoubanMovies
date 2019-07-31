import requests
from urllib.parse import quote
from json.decoder import JSONDecodeError
from storage.mysql import Mysql
from config import *
import random
from storage.redis import RedisClient


class Spider:

    def __init__(self, page_start):
        self.page_start = page_start
        self.headers = {
            "Connection": "Keep-alive",
            'User-Agent': random.choice(USER_AGENTS),
        }
        self.proxies = None
        self.redis = RedisClient()

        self.db = Mysql()
        self.tableName = TABLENAME
        self.tag = TAG

    def test_proxy(self):
        """
        二次清洗代理
        :return:健康代理
        """
        url = 'https://movie.douban.com/subject/1292052/'
        proxy = self.redis.proxy_random()
        self.proxies = {
            'http': 'http://' + proxy,
            'https': 'https://' + proxy
        }
        try:
            r = requests.get(url, headers=self.headers, allow_redirects=False, proxies=self.proxies, timeout=20)
            if r.status_code == 200:
                print("*********************代理有效：", proxy)
            else:
                self.redis.proxy_decrease(proxy)
                self.test_proxy()
        except:
            self.redis.proxy_decrease(proxy)
            self.test_proxy()

    def get_page_index(self, page_start):
        url = 'https://movie.douban.com/j/search_subjects?type=movie&tag={0}&sort=rank&page_limit=20&page_start={1}'.format(
            quote(self.tag), page_start)
        try:
            response = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=20)
            print("正在爬取：", url)
            if response and response.status_code == 200:
                self.parse_page_index(response)
            if response.status_code == 302:
                response = requests.get(url, headers=self.headers, proxies=self.proxies, allow_redirects=False,
                                        timeout=20)
                print('重新爬取：', url)
                self.parse_page_index(response)
        except:
            self.test_proxy()
            self.get_page_index(page_start)

    def parse_page_index(self, response):
        try:
            # response.raise_for_status()
            json_dict = response.json()
            if json_dict and json_dict.get('subjects'):
                for item in json_dict.get('subjects'):
                    # yield item.get('id')
                    if item.get('id'):
                        self.redis.add(REQUEST_REDIS_KEY, item.get('id'))
                        print("获取id：", item.get("id"))
        # except ConnectionError or RequestException:
        #     print('网页请求失败：', url)
        except JSONDecodeError:
            print("response不是json格式的数据")

    def run(self):
        # if not self.proxies:
        #     self.test_proxy()
        self.get_page_index(self.page_start)

