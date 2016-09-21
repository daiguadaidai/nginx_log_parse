#!/usr/bin/env python
#-*- coding: utf-8 -*-

from ng_line_parser import NgLineParser

import pandas as pd
import os
import sys

class PDNgLogStat(object):

    def __init__(self):
        self.ng_line_parser = NgLineParser()

    def _log_line_iter(self, path):
        """解析文件中的每一行并生成一个迭代器"""
        with open(path, 'r') as f:
            for index, line in enumerate(f):
                self.ng_line_parser.parse(line)
                yield self.ng_line_parser.to_dict()

    def load_data(self, path):
        """通过给的文件路径加载数据生成 DataFrame"""
        self.df = pd.DataFrame(self._log_line_iter(path))

    def pv_day(self):
        """计算每一天的 PV"""
        group_by_cols = ['access_time'] # 需要分组的列,只计算和显示该列
        
        # 下面我们是按 yyyy-mm-dd 形式来分组的, 所以需要定义分组策略:
        # 分组策略为: self.df['access_time'].map(lambda x: x.split()[0])
        pv_day_grp = self.df[group_by_cols].groupby(
                       self.df['access_time'].map(lambda x: x.split()[0]))
        return pv_day_grp.agg(['count'])

    def pv_hour(self):
        """计算在一天当中每个时段的访问情况"""
        group_by_cols = ['access_time'] # 需要分组的列,只计算和显示该列
        
        # 下面我们是按 hh(小时) 形式来分组的, 所以需要定义分组策略:
        # 分组策略为: self.df['access_time'].map(lambda x: x.split().pop().split(':')[0])
        pv_hour_grp = self.df[group_by_cols].groupby(
                       self.df['access_time'].map(lambda x: x.split().pop().split(':')[0]))
        return pv_hour_grp.agg(['count'])

    def url_ref_stat(self):
        """统计外链点击情况"""
        group_by_cols = ['reference_url'] # 需要分组的列,只计算和显示该列
        
        # 直接统计次数
        url_ref_grp = self.df[group_by_cols].groupby(
                                     self.df['reference_url'])
        return url_ref_grp.agg(['count'])['reference_url']['count'].sort_values(ascending=False)

    def url_req_stat(self):
        """统计那个页面点击量"""
        group_by_cols = ['request_url'] # 需要分组的列,只计算和显示该列
        
        # 直接统计次数
        url_req_grp = self.df[group_by_cols].groupby(
                                     self.df['request_url'])
        return url_req_grp.agg(['count'])['request_url']['count'].sort_values(ascending=False)

    def uv_cdn_ip(self):
        """统计cdn ip量"""
        group_by_cols = ['cdn_ip'] # 需要分组的列,只计算和显示该列
        
        # 直接统计次数
        url_req_grp = self.df[group_by_cols].groupby(
                                     self.df['cdn_ip'])
        return url_req_grp.agg(['count'])['cdn_ip']['count'].nlargest(100)

    def uv_real_ip(self):
        """统计cdn ip量"""
        group_by_cols = ['real_ip'] # 需要分组的列,只计算和显示该列
        
        # 直接统计次数
        url_req_grp = self.df[group_by_cols].groupby(
                                     self.df['real_ip'])
        return url_req_grp.agg(['count'])['real_ip']['count'].nlargest(100)

    def browser_stat(self):
        """统计不同浏览器访问次数"""
        group_by_cols = ['browser'] # 需要分组的列,只计算和显示该列
        
        # 直接统计次数
        url_req_grp = self.df[group_by_cols].groupby(
                                     self.df['browser'])
        return url_req_grp.agg(['count'])['browser']['count'].nlargest(100)

def main():          
    file_path = 'www.ttmark.com.access.log'

    pd_ng_log_stat = PDNgLogStat()
    pd_ng_log_stat.load_data(file_path)

    # 统计每日 pv
    # print pd_ng_log_stat.pv_day()

    # 统计每小时 pv
    # print pd_ng_log_stat.pv_hour()

    # 统计外链点击情况
    # print pd_ng_log_stat.url_ref_stat()

    # 统计页面点击量
    # print pd_ng_log_stat.url_req_stat()

    # 统计 CDN IP 访问量
    # print pd_ng_log_stat.uv_cdn_ip()

    # 统计 真实用户 IP 访问量
    # print pd_ng_log_stat.uv_real_ip()

    # 统计 统计不同浏览器访问次数
    print pd_ng_log_stat.browser_stat()


if __name__ == '__main__':
    main()
