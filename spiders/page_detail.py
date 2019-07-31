import random
import requests
from lxml import etree
import re
from storage.mysql import Mysql
from storage.redis import RedisClient
from config import *


class Page:
    def __init__(self):
        self.headers = {
            "Connection": "Keep-alive",
            'User-Agent': random.choice(USER_AGENTS),
        }
        self.proxies = None
        self.redis = RedisClient()

        self.db = Mysql()
        self.tableName = TABLENAME
        self.requestKey = REQUEST_REDIS_KEY

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

    def get_page_detail(self):
        while not self.redis.empty(self.requestKey):
            id = self.redis.pop(self.requestKey)
            url = 'https://movie.douban.com/subject/{}'.format(id)
            try:
                response = requests.get(url, headers=self.headers, proxies=self.proxies, allow_redirects=True,
                                        timeout=20)
                print('正在爬取：', url)
                if response and response.status_code == 200:
                    self.parse_page_detail(id, response.text)
                else:
                    print("等待重新爬取：", url)
                    self.test_proxy()
                    self.redis.add(self.requestKey, id)
            except IndexError:
                print("文章解析错误：", url)
                # 把解析错误的文章的ID存起来
                self.redis.add(ERROR_REQUEST_REDIS_KEY, id)
            except:
                print("等待重新爬取：", url)
                self.test_proxy()
                self.redis.add(self.requestKey, id)

    def parse_page_detail(self, id, response):
        html = etree.HTML(response)
        title = html.xpath('//*[@id="content"]/h1/span[1]/text()')[0]
        info_element = html.xpath('//*[@id="info"]')[0]
        director = ' / '.join(info_element.xpath('.//a[@rel="v:directedBy"]/text()'))
        writer = ' / '.join(info_element.xpath('./span[2]/span[@class="attrs"]/a/text()')) if info_element.xpath('./span[2]/span[@class="attrs"]/a/text()') else ""
        actor = ' / '.join(info_element.xpath('//span[@class="actor"]//a/text()'))
        genre = ' / '.join(info_element.xpath('//span[@property="v:genre"]/text()'))
        produce_area = re.findall(re.compile('<span class="pl">制片国家/地区:</span>(.*?)<br/>'), response)[0].strip()
        language = re.findall(re.compile('<span class="pl">语言:</span>(.*?)<br/>'), response)[0].strip()
        release_date = ' / '.join(info_element.xpath('//span[@property="v:initialReleaseDate"]/text()')) if info_element.xpath('//span[@property="v:initialReleaseDate"]/text()') else ""
        # runtime = info_element.xpath('//span[@property="v:runtime"]/text()')[0]
        if info_element.xpath('//span[@property="v:runtime"]/text()'):
            runtime = info_element.xpath('//span[@property="v:runtime"]/text()')[0]
        elif re.findall(re.compile('<span class="pl">片长:</span>(.*?)<br/>'), response):
            runtime = re.findall(re.compile('<span class="pl">片长:</span>(.*?)<br/>'), response)[0].strip()
        else:
            runtime = ""
        # alias = re.findall(re.compile('<span class="pl">又名:</span>(.*?)<br/>'), response)[0].strip()
        alias = re.findall(re.compile('<span class="pl">又名:</span>(.*?)<br/>'), response)[0].strip() if re.findall(re.compile('<span class="pl">又名:</span>(.*?)<br/>'), response) else ""

        imdb = re.findall(re.compile('<span class="pl">IMDb链接:</span> <a.*?>(.*?)</a>'), response)[0].strip() if re.findall(re.compile('<span class="pl">IMDb链接:</span> <a.*?>(.*?)</a>'), response) else ""
        rate = html.xpath('//strong[@property="v:average"]/text()')[0]
        voters = html.xpath('//span[@property="v:votes"]/text()')[0]
        five_stars, four_stars, three_stars, two_stars, one_star = html.xpath('//span[@class="rating_per"]/text()')
        intro = html.xpath('//span[@property="v:summary"]/text()')[0].strip() if html.xpath('//span[@property="v:summary"]/text()') else ""
        collections, wishes = html.xpath('//div[@class="subject-others-interests-ft"]/a/text()')
        # item = [id,rate,title,director,writer,actor,genre,produce_area,language,release_date,runtime,alias,imdb,intro,voters,five_stars,four_stars,three_stars,two_stars,one_star,collections,wishes]
        item = {
            "id": id, "rate": rate, "title": title, "director": director, "writer": writer, "actor": actor,
            "genre": genre, "produce_area": produce_area, "language": language,
            "release_date": release_date, "runtime": runtime, "alias": alias, "imdb": imdb, "intro": intro,
            "voters": voters, "five_stars": five_stars,
            "four_stars": four_stars, "three_stars": three_stars, "two_stars": two_stars, "one_star": one_star,
            "collections": collections, "wishes": wishes
        }
        # yield item
        self.save_to_mysql(item)

    def save_to_mysql(self, item):
        keys = ', '.join(item.keys())
        values = ', '.join(['%s'] * len(item))
        insert_sql = 'insert into %s (%s) values (%s)' % (self.tableName, keys, values)
        self.db.modify(insert_sql, tuple(item.values()))
        print("-----------插入成功：", item.get('title'))

    def run(self):
        if not self.proxies:
            self.test_proxy()
        self.get_page_detail()
