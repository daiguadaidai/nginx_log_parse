#!/usr/bin/env python
#-*- coding:utf-8 -*-

import datetime
import urllib

class NgLineParser(object):
    """将 Nginx 日志解析成多个字段"""

    def __init__(self):
        self._cdn_ip = '' # CDN请求IP
        self._access_time = '' # 请求时间
        self._request_url = '' # 请求的URL
        self._reference_url = '' # 外链URL
        self._response_status = '' # NG 响应状态码
        self._browser = '' # 用户使用的浏览器
        self._real_ip = '' # 用户真实IP

    def parse(self, line):
        """通过传入的一行数据进行解析
        """
        line_item = line.strip().split('"')

        if len(line_item) > 9: # 由于日志有改变需要删除一些元素
            del line_item[1]
            del line_item[1]

        # 获取临时的 CDN IP 和 访问文件
        ip_time_tmp = line_item[0].strip().split()

        self.real_ip = line_item[7] # 用户真实IP
        self.cdn_ip = ip_time_tmp[0] # CDN请求IP
        self.access_time = ip_time_tmp[3].lstrip('[') # 请求时间
        self.request_url = line_item[1].strip().split()[1] # 请求的URL
        self.reference_url = line_item[3].strip() # 外链URL
        self.response_status = line_item[2].strip().split()[0] # NG 响应状态码
        self.browser = ' '.join(line_item[5].strip().split()[-2:]) # 用户使用的浏览器

    def to_dict(self):
        """将属性(@property)的转化为dict输出
        """
        propertys = {}
        propertys['real_ip'] = self.real_ip
        propertys['cdn_ip'] = self.cdn_ip
        propertys['access_time'] = self.access_time
        propertys['request_url'] = self.request_url
        propertys['reference_url'] = self.reference_url
        propertys['response_status'] = self.response_status
        propertys['browser'] = self.browser

        return propertys

    @property
    def real_ip(self):
        return self._real_ip

    @real_ip.setter
    def real_ip(self, real_ip):
        self._real_ip = real_ip.split(', ')[0]

    @property
    def browser(self):
        return self._browser

    @browser.setter
    def browser(self, browser):
        self._browser = browser

    @property
    def response_status(self):
        return self._response_status

    @response_status.setter
    def response_status(self, response_status):
        self._response_status = response_status

    @property
    def reference_url(self):
        return self._reference_url

    @reference_url.setter
    def reference_url(self, reference_url):
        """解析外链URL
        只需要解析后的域名, 如:
            传入: http://www.ttmark.com/diannao/2014/11/04/470.html
            解析成: www.ttmark.com
        """
        proto, rest = urllib.splittype(reference_url)
        res, rest = urllib.splithost(rest)
        if not res:
            self._reference_url = '-'
        else:
            self._reference_url = res

    @property
    def request_url(self):
        return self._request_url

    @request_url.setter
    def request_url(self, request_url):
        """解析请求的URL
        只需要解析后的URL路径不需要参数, 如:
            传入: /wp-admin/admin-ajax.php?postviews_id=1348
            解析成: /wp-admin/admin-ajax.php
        """
        proto, rest = urllib.splittype(request_url)
        url_path, url_param = urllib.splitquery(rest)

        if url_path.startswith('/tag/'):
            url_path = '/tag/'

        self._request_url = url_path

    @property
    def access_time(self):
        return str(self._access_time)

    @access_time.setter
    def access_time(self, access_time):
        # Nginx log 解析日志格式
        input_datetime_format = '%d/%b/%Y:%H:%M:%S'

        self._access_time = datetime.datetime.strptime(
                                     access_time,
                                     input_datetime_format)

    @property
    def cdn_ip(self):
        return self._cdn_ip

    @cdn_ip.setter
    def cdn_ip(self, cdn_ip):
        self._cdn_ip = cdn_ip


def main():
    ng_line_parser = NgLineParser()

    with open('www.ttmark.com.access.log', 'r') as f:
        for index, line in enumerate(f):
            ng_line_parser.parse(line)

            print '----------------------------'
            print index
            print ng_line_parser.real_ip
            print ng_line_parser.cdn_ip
            print ng_line_parser.access_time
            print ng_line_parser.request_url
            print ng_line_parser.reference_url
            print ng_line_parser.response_status
            print ng_line_parser.browser

            # 只解析10行
            if index == 10: break


if __name__ == '__main__':
    main()
