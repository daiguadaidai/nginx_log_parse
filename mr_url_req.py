# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
from ng_line_parser import NgLineParser

import heapq

class MRUrlReq(MRJob):

    ng_line_parser = NgLineParser()

    def mapper(self, _, line):
        self.ng_line_parser.parse(line)
        yield self.ng_line_parser.request_url, 1 # 请求网页

    def reducer_sum(self, key, values):
        yield None, [sum(values), key]

    def reducer_top100(self, _, values):
        """访问数降序"""
        for cnt, path in heapq.nlargest(100, values):
            yield cnt, path

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_top100)
        ]


def main():
    MRUrlReq.run()

if __name__ == '__main__':
    main()

