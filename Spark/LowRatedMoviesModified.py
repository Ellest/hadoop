from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import asc

def parseLine(line):
    tokens = line.value.split()
    return Row(movieID=int(tokens[1]),
               rating=float(tokens[2]))

if __name__ == '__main__':

    spark = SparkSession.builder.appName('modified').getOrCreate()

    # meta data
    itemRDD = spark.read.text('ml-100k/u.item').rdd
    itemRDD = itemRDD.map(lambda x: x.value.split('|'))
    itemRDD = itemRDD.map(lambda x: Row(movieID=x[0], movieName=x[1]))
    itemDF = spark.createDataFrame(itemRDD)

    # main data
    ratingRDD = spark.read.text('hdfs:///user/maria_dev/ml-100k/u.data').rdd
    ratingRDD = ratingRDD.map(parseLine)
    ratingDF = spark.createDataFrame(ratingRDD)

    # count and avg
    countDF = ratingDF.groupBy('movieID').count().filter('count >= 10')
    ratingDF = ratingDF.groupBy('movieID').avg('rating').filter('avg(rating) < 2.0')

    # merge avg and count then take worst 10
    ratingDF = countDF.join(ratingDF, ['movieID'], 'inner')
    ratingDF = spark.createDataFrame(ratingDF.sort(asc('avg(rating)')).take(10))

    # merging with meta data
    resultDF = ratingDF.join(itemDF, ['movieID'], 'inner').sort(asc('avg(rating)'))
    resultDF.show()

    # stop session 
    spark.stop()