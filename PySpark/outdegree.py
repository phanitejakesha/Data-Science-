from pyspark import SparkContext
import sys

sc = SparkContext("local", "app")

def flatMapHelper(line):
    x = line.split("\t")
    return [x[0]]



text_file = sc.textFile(sys.argv[1])
counts = text_file.flatMap(flatMapHelper) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b).sortByKey()
counts.saveAsTextFile(sys.argv[2])

