代理池+进程池+异步多线程（Redis）爬取豆瓣评分前500的电影



## 代码逻辑

1. 爬取[豆瓣高分top500](https://movie.douban.com/j/search_subjects?type=movie&tag=%E8%B1%86%E7%93%A3%E9%AB%98%E5%88%86&sort=rank&page_limit=20&page_start=0)，每页20条

   

2. 进程池 + 异步多线程。

   开启进程池，每个进程下开启两个线程，一个爬取电影列表页，提取每部电影ID并存进Redis；另一个线程从Redis列表中取出ID，拼接成电影详情页URL，爬取、解析并存进MySQL数据库中，实现异步多线程。

   

3. 基于Redis有序集合，维护一个代理IP池。

   爬取免费代理IP并赋予初始得分score，每次从得分最高的IP中随机取出一个进行健康测试，如果不能使用则减一分直至为0时从Redis中剔除。



## 环境

Redis，MySQL，Python3.5+

requirements，个人使用的版本，仅作参考。

```python
redis>=3.2.1
requests>=2.21.0
lxml>=4.2.5
pymysql>=0.9.3
```



## 参考

代理池：<https://github.com/merelysmile/ProxyPool>

代理池的实现借鉴了 [崔庆才老师的项目](https://github.com/Python3WebSpider/ProxyPool)，在此基础上更新了免费代理网站的爬取规则。

代理IP的测试与使用，参考了 [utopianist的项目](https://github.com/utopianist/SougouWeixin)