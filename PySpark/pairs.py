from pyspark import SparkContext
import sys

sc = SparkContext("local", "app")


def firstElement(line):
    x = line.split("\t")
    return [[x[0],x[1]]]


def mapperFunction1(word):
    node1 = word[0]
    node2 = word[1]
    hashedValue = node1+node2
    globalDict.add(hashedValue)
    if node2+node1 in globalDict:
        return (node1,[node2])
    return ('phani',1)
    


def mapperFunction2(word):
    node1 = word[0]
    node2 = word[1]
    hashedValue = node1+node2
    globalDict.add(hashedValue)
    if node2+node1 in globalDict:
        return (node2,[node1])
    return ('phani',1)


text_file = sc.textFile(sys.argv[1])
globalDict = set()
counts1 = text_file.flatMap(firstElement) \
             .map(mapperFunction1) \
             .reduceByKey(lambda a, b: a + b).sortByKey()

globalDict = set()
counts2 = text_file.flatMap(firstElement) \
             .map(mapperFunction2) \
             .reduceByKey(lambda a, b: a + b).sortByKey()


merged = counts1.union(counts2)

merged.saveAsTextFile(sys.argv[2])

