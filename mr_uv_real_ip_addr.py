#!/usr/bin/env python
#-*- coding:utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from ng_line_parser import NgLineParser

import pandas as pd
import heapq
import socket
import struct
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


class MRUVRrealIpAddr(MRJob):

    OUTPUT_PROTOCOL = RawProtocol
    ng_line_parser = NgLineParser()

    def mapper(self, _, line):
        self.ng_line_parser.parse(line)
        yield self.ng_line_parser.real_ip, 1

    def reducer_sum(self, key, values):
        """统计 VU"""
        yield None, [str(sum(values)), key]

    def init_ip_addr_df(self):
        """读取IP Addr 文件构造 DataFrame 文件"""
        cols = ['id', 'ip_start_num', 'ip_end_num',
                'ip_start', 'ip_end', 'addr', 'operator']
        area_ip_path = '/root/script/nginx_log_parse/area_ip.csv'     
        self.ip_addr_df = pd.read_csv(area_ip_path, sep='\t', names=cols, index_col='id')

    def reducer_top100(self, _, values):
        """访问数降序"""
        for cnt, ip in heapq.nlargest(100, values, key=lambda x: int(x[0])):
            ip_num = -1
            try:
                # 将IP转化成INT/LONG 数字
                ip_num = socket.ntohl(struct.unpack("I",socket.inet_aton(str(ip)))[0])
                # 通过数字获得 地址 DataFrame
                addr_df = self.ip_addr_df[(self.ip_addr_df.ip_start_num <= ip_num) & 
                                          (ip_num <= self.ip_addr_df.ip_end_num)]
                # 通过索引值获得获得 地址
                addr = addr_df.at[addr_df.index.tolist()[0], 'addr']
                yield cnt, '{ip}    {addr}'.format(ip=ip, addr=addr)
            except:
                yield cnt, ip 

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum),
            MRStep(reducer_init = self.init_ip_addr_df,
                   reducer=self.reducer_top100)
        ]


def main():
    MRUVRrealIpAddr.run()

if __name__ == '__main__':
    main()
