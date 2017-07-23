#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ----------------------------------------------
# Apache Beam Pipeline
# To find the 10 most common words in Hamlet
# ----------------------------------------------

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# ----------------------------------------------
# MRJOB
# https://pythonhosted.org/mrjob/guides/quickstart.html
# ----------------------------------------------

from mrjob.job import MRJob
from mrjob.step import MRstep

import heapq
import re


WORD_RE = re.compile(r"[\w']+")
TOPN = 1

class WordCountTop(MRJob):
    def mapper(self, _, line):
        for word in line.strip().lower().split():
			yield filter(str.isalpha(word), 1)
    def reducer(self, word, counts):
		yield word, sum(counts)
    def topN_mapper(self, _, line):
        yield "TOP" + str(TOPN), (count, word)
    def topN_reducer(self, word, counts):
        for countAndWord in heapq.nlargest(TOPN, countsAndWords):
            yield _, countAndWord
    def steps(self):
        return[
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(mapper = self.topN_mapper,
                	reducer = self.topN_reducer)]

# ----------------------------------------------
# Defining functions
# ----------------------------------------------

def read_words(line):
	for word in line.strip().lower().split():
		yield (word, 1)

def by_value(kv1, kv2):
	key1, value1 = kv1
	key2, value2 = kv2
	return value1 < value2 

# ----------------------------------------------
# Beam Pipeline 
# ----------------------------------------------

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='')
	parser.add_argument('--input', default='', help='Input directory')
	parser.add_argument('--output', default='', help='Output directory')

	options, pipeline_args = parser.parse_known_args()
	p = beam.Pipeline(argv=pipeline_args)

	input = '{0}hamlet.txt'.format(options.input)
	output = options.output

	(p
	  | 'Input' >> beam.io.ReadFromText(input)
	  | 'Read' >> beam.FlatMap(lambda line: read_words(line))
	  | 'TotalUse' >> beam.CombinePerKey(sum)
	  | 'Top_5' >> beam.transforms.combiners.Top.Of(10, by_value)
	  | 'write' >> beam.io.WriteToText(output)
	)

	p.run()

	WordCountTop.run()

	