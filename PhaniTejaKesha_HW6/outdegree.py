from pyspark import SparkContext
import sys

sc = SparkContext("local", "app")

text_file = sc.textFile(sys.argv[1])
counts = text_file.map(lambda line:line.split("\t")[0]) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(sys.argv[2])
