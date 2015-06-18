#!/usr/bin/env python
import codecs
import os.path
from pprint import pprint
import re
import requests
from pyspark import SparkContext

filename = '/tmp/moby.txt'

if not os.path.isfile(filename):
    r = requests.get('http://www.gutenberg.org/cache/epub/2701/pg2701.txt')
    with codecs.open(filename, 'w', 'utf-8') as f:
        f.write(r.text)

sc = SparkContext('local[*]')  # '*' means all CPUs ... local[2] would use 2

wordcounts = sc.textFile(filename) \
    .map(lambda text: re.sub('[^a-z0-9 ]+', '', text.lower()).strip()) \
    .flatMap(lambda text: text.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y)  # Transformations

pprint(wordcounts.takeOrdered(10, key=lambda (k, v): -v))  # Action
