from pyspark import SparkContext
import sys

sc = SparkContext("local", "app")


def flatMapHelper(line):
    x = line.split("\t")
    return [[x[0],x[1]]]


def mapperHelper1(word):
    node1 = word[0]
    node2 = word[1]
    hashedValue = node1+"->"+node2
    globalDict.add(hashedValue)
    if node2+"->"+node1 in globalDict:
        return (node1,[node2])
    return ('NoneVal',1)
    


def mapperHelper2(word):
    node1 = word[0]
    node2 = word[1]
    if node1 == node2:
        return ('NoneVal',1)
    hashedValue = node1+"->"+node2
    globalDict.add(hashedValue)
    if node2+"->"+node1 in globalDict:
        return (node2,[node1])
    return ('NoneVal',1)



text_file = sc.textFile(sys.argv[1])
globalDict = set()
counts1 = text_file.flatMap(flatMapHelper) \
             .map(mapperHelper1).filter(lambda keyval: keyval!='NoneVal') \
             .reduceByKey(lambda a, b:a + b).sortByKey()
globalDict = set()
counts2 = text_file.flatMap(flatMapHelper) \
             .map(mapperHelper2).filter(lambda keyval: keyval!='NoneVal') \
             .reduceByKey(lambda a,b:a+b).sortByKey()


merged = counts1.union(counts2).reduceByKey(lambda a,b:a+b).sortByKey()

merged.saveAsTextFile(sys.argv[2])

