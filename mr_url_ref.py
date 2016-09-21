# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
from ng_line_parser import NgLineParser

import heapq

class MRUrlRef(MRJob):

    ng_line_parser = NgLineParser()

    def mapper(self, _, line):
        self.ng_line_parser.parse(line)
        yield self.ng_line_parser.reference_url, 1 # 外链域名

    def reducer_sum(self, key, values):
        """统计 VU"""
        yield None, [sum(values), key]

    def reducer_desc(self, key, values):
        """访问数降序"""
        for cnt, value in sorted(values, reverse=True):
            yield cnt, value

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_desc)
        ]


def main():
    MRUrlRef.run()

if __name__ == '__main__':
    main()

