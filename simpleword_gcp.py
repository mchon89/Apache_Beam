#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ----------------------------------------------
# Apache Beam Pipeline but in Dataflow in GCP
# To find the 10 most common words in Hamlet
# ----------------------------------------------

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

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
# Running Beam Pipeline in GCP 
# Change Project and Bucket names
# ----------------------------------------------

PROJECT='cloud-training-demos' 
BUCKET='cloud-training-demos'

def run():
	argv = [
	'--project={0}'.format(PROJECT),
	'--job_name=examplejob2',
	'--save_main_session',
	'--staging_location=gs://{0}/staging/'.format(BUCKET),
	'--temp_location=gs://{0}/staging/'.format(BUCKET),
	'--runner=DataflowRunner'
	]

	p = beam.Pipeline(argv=argv)
	input = 'gs://{0}/hamlet.txt'.format(BUCKET)
	output_prefix = 'gs://{0}/'.format(BUCKET)

	(p
	  | 'Input' >> beam.io.ReadFromText(input)
	  | 'Read' >> beam.FlatMap(lambda line: read_words(line))
	  | 'TotalUse' >> beam.CombinePerKey(sum)
	  | 'Top_5' >> beam.transforms.combiners.Top.Of(10, by_value)
	  | 'write' >> beam.io.WriteToText(output)
	)

	p.run()

if __name__ == '__main__':
	run()
