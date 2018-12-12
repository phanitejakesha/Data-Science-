from pyspark import SparkContext
import sys

sc = SparkContext("local", "app")

def firstElement(line):
    x = line.split("\t")
    return [x[0]]



text_file = sc.textFile(sys.argv[1])
counts = text_file.flatMap(firstElement) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(sys.argv[2])

