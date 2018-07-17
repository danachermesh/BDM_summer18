from pyspark import SparkContext
import sys
import csv


def extractRest(partId, records):
    if partId==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (camis, cuisine) = (row[0], row[7])
        yield (camis, cuisine)



if __name__=='__main__':
    sc = SparkContext()

    NYC_CUIS = sys.argv[-2]

    cuis = sc.textFile(NYC_CUIS, use_unicode=True).cache()

    nycResturants = cuis.mapPartitionsWithIndex(extractRest)

    nycResturants \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda x,y: 1) \
            .keys().map(lambda x: (x[1],1)) \
            .reduceByKey(lambda x,y: x+y) \
            .sortBy(lambda x: -x[1]) \
            .saveAsTextFile(sys.argv[-1])
