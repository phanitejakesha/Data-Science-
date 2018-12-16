from pyspark import SparkContext
import sys

sc = SparkContext("local", "app")

def flatMapHelper(line):
    x = line.split("\t")
    return [[x[1],x[2]]]


def mapHelper(word):
    node1 = word[0]
    w = int(word[1])
    return (node1,w)



text_file = sc.textFile(sys.argv[1])
counts = text_file.flatMap(flatMapHelper) \
             .map(mapHelper) \
             .reduceByKey(lambda a, b: a + b).sortByKey()
counts.saveAsTextFile(sys.argv[2])

