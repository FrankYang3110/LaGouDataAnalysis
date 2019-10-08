#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import time
import json
import random
import requests
import pymysql
from math import ceil
from lxml import etree


class LaGouSpider(object):
    def __init__(self):
        self.post_url = 'https://www.lagou.com/jobs/positionAjax.json?px=default&needAddtionalResult=false'
        self.single_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'}
        self.post_headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://www.lagou.com/jobs/list_%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90?labelWords=&fromSearch=true&suginput=',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36',
            'X-Anit-Forge-Code': '0',
            'X-Anit-Forge-Token': 'None',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.session = requests.session()
        self.detail_url_format = 'https://www.lagou.com/jobs/{positionID}.html'
        self.conn = pymysql.Connect(host='localhost', user='root', password='yangchao', database='frank', charset='utf8')
        self.cursor = self.conn.cursor()

    def get_page_count(self):  # 通过抓取ajax请求查看数据一共有多少页
        first_data = {
            'first': 'false',
            'pn': '1',
            'kd': '数据分析师'
        }
        url = 'https://www.lagou.com/jobs/list_%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90?labelWords=' \
              '&fromSearch=true&suginput= HTTP/1.1'  # 拉钩反爬，在请求主页的时候，必须先请求一次主页，以获取cookie
        res = self.session.get(url, headers=self.single_headers)
        if res.status_code == 200:
            resp = self.session.post(self.post_url, data=first_data, headers=self.post_headers)
            data = json.loads(resp.text)
            total_count = data.get("content").get('positionResult').get('totalCount')
            if total_count % 15 == 0:
                total_page = total_count / 15
            else:
                total_page = ceil(total_count / 15)
            return total_page

    def get_data(self, total_page):  # 获取到列表页面的数据
        info_dict = {}
        # 主url
        url1 = 'https://www.lagou.com/jobs/list_python?city=%E5%85%A8%E5%9B%BD&cl=' \
               'false&fromSearch=true&labelWords=&suginput='
        # ajax请求
        url = "https://www.lagou.com/jobs/positionAjax.json?px=d" \
              "efault&needAddtionalResult=false"
        # 请求头
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'https://www.lagou.com/jobs/list_python?px=default&city=%E5%85%A8%E5%9B%BD',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            'Host': 'www.lagou.com'
        }
        # 通过提交参数data来控制翻页

        for page in range(3, total_page):
            data = {
                'first': 'false',
                'pn': page,
                'kd': '数据分析师'
            }
            self.session.get(url=url1, headers=headers, timeout=3)  # 请求主页获取cookie
            cookie = self.session.cookies
            respon = self.session.post(url=url, headers=headers, data=data, cookies=cookie, timeout=3)  #请求ajax获取positionid
            time.sleep(5 * (random.random()))
            data = json.loads(respon.text)
            self.get_position_id(data)

    def get_position_id(self, response_data):  # 根据列表页数据拿到positionId
        result = response_data.get("content").get('positionResult').get('result')
        for position in result:
            position_id = position.get('positionId')
            try:
                self.get_detail_data(position_id)
            except Exception as e:
                print(e, position_id)

    def get_detail_data(self, position_id):  # 根据positionId构建出详情页面的url，并解析出需要的数据。
        info_dict = {}
        url = self.detail_url_format.format(positionID=position_id)
        time.sleep(3* (random.random()))
        res = self.session.get(url, headers=self.single_headers, timeout=10)
        if res.status_code == 200:
            res.encoding = res.apparent_encoding
            tree = etree.HTML(res.text)
            name = tree.xpath('//div[@class="job-name"]/@title')[0]  # 职位名称
            salary = tree.xpath('//span[@class="salary"]/text()')[0]  # 月薪
            company_name = tree.xpath('//em[@class="fl-cn"]/text()')[0].strip()  # 公司名称
            industry = tree.xpath('//h4[@class="c_feature_name"]/text()')[0]  # 不同行业对数据分析师的需求
            city = tree.xpath('//div[@class="work_addr"]/a[1]/text()')[0]  # 不同城市需求量大小
            experience = tree.xpath('//dd[@class="job_request"]/h3/span[3]/text()')[0].replace('/', '').strip()  # 所需工作经验
            education = tree.xpath('//dd[@class="job_request"]/h3/span[4]/text()')[0].replace('/', '').strip()  # 学历要求
            skill = ''.join(tree.xpath('//div[@class="job-detail"]//text()'))  # 技能需求

            info_dict.update(id=position_id)
            info_dict.update(name=name)
            info_dict.update(salary=salary)
            info_dict.update(company_name=company_name)
            info_dict.update(industry=industry)
            info_dict.update(city=city)
            info_dict.update(experience=experience)
            info_dict.update(education=education)
            info_dict.update(skill=skill)

            self.insert_mysql(info_dict)

    def insert_mysql(self, info_dict):  # 数据入库
        try:
            sql = """
            insert into job_copy values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.execute(sql, [info_dict.get('id'), info_dict.get('name'), info_dict.get('salary'),
                                      info_dict.get('company_name'),info_dict.get('industry'), info_dict.get('city'),
                                      info_dict.get('experience'), info_dict.get('education'), info_dict.get('skill')])
            self.conn.commit()
        except Exception as e:
            print('insert into mysql error:', e)


if __name__ == '__main__':
    spider = LaGouSpider()
    total_page = spider.get_page_count()
    spider.get_data(total_page)

