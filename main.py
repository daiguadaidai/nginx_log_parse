#!/usr/bin/env python
#-*- coding: utf-8 -*-

from pd_ng_log_stat import PDNgLogStat

import argparse
import os


def filter_eff_files(files):
    """过滤有效文件"""
    return [path for path in files if os.path.exists(path)]

def filter_eff_file(file):
    """过滤有效文件"""
    if os.path.exists(file):
        return file
    else:
        return None

def get_eff_methods_no_addr():
    """获得可以执行的方法 除了需要IP地址文件的方法"""
    methods = [
        'pv_day', # 统计每日 pv
        'pv_hour', # 统计每小时 pv
        'url_ref_stat', # 统计外链点击情况
        'url_req_stat', # 统计页面点击量
        'uv_cdn_ip', # 统计 CDN IP 访问量
        'uv_real_ip', # 统计 真实用户 IP 访问量
        'browser_stat', # 统计 统计不同浏览器访问次数
    ]

    return methods
    
def get_eff_methods():
    """获得可以执行的方法"""
    methods = get_eff_methods_no_addr()
    methods.append('uv_cdn_ip_addr') # 统计 CDN IP 访问量 和 地址
    methods.append('uv_real_ip_addr') # 统计 用户真实 IP 访问量 和 地址

    return methods

def filter_methods_no_addr(methods):
    return list(set(get_eff_methods_no_addr()).intersection(set(methods)))

def filter_methods(methods):
    return list(set(get_eff_methods()).intersection(set(methods)))

def main():
    parser = argparse.ArgumentParser(description='parse nginx log')
    
    # 文件参数
    parser.add_argument('-f', '--files', required = True,
                        help = 'specified file one or multi file')
    # 运行那个方法
    parser.add_argument('-m', '--methods', required = True,
                        help = 'specified method to run')
    # ip 对应地址的 路径
    parser.add_argument('-i', '--ipfile',
                        help = 'specified ip address file')

    # 解析并检查参数是否合法
    args = parser.parse_args()

    # 获取有效文件
    files = filter_eff_files(args.files.split())
    area_ip_path = None
    if args.ipfile:
        area_ip_path = filter_eff_file(args.ipfile) # 判断ip文件是否可用

    # 获取需要运行的方法
    methods = args.methods.split()

    # 创建统计实例
    pd_ng_log_stat = PDNgLogStat()
    pd_ng_log_stat.load_data(files)

    # 判断是是否有 ip文件 并加载相关执行方法
    if area_ip_path:
        methods = filter_methods(methods) # 和所有方法过滤
        pd_ng_log_stat.load_ip_addr(area_ip_path) # 加载IP文件
    else:
        methods = filter_methods_no_addr(methods) # 去除需要Ip文件的方法

    for method in methods:
        print eval('pd_ng_log_stat.{method}()'.format(method = method))


if __name__ == '__main__':
    main()
