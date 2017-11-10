from pyspark.sql import SparkSession, Row
from pyspark.ml.recommendation import ALS #ALS Rec lib
from pyspark.sql.functions import *

def parseLine(line):
    tokens = line.value.split()
    return Row(userID=int(tokens[0]), movieID=int(tokens[1]), rating=float(tokens[2]))

if __name__ == '__main__':
    
    spark = SparkSession.builder.appName('Rec').getOrCreate()

    # RDD of strings
    #rawMeta = spark.sparkContext.textFile('ml-100k/u.item')
    rawMeta = spark.read.text('ml-100k/u.item').rdd

    # generate a cleaned RDD by parsing 
    cleanedMeta = rawMeta.map(lambda x: x.value.split('|'))

    # extract only need columns into a new RDD
    rows = cleanedMeta.map(lambda x: Row(movieID=x[0], movieName=x[1])) 

    # create and cache metaDF
    metaDF = spark.createDataFrame(rows).cache()

    # spark.read.text returns a DF
    # .rdd converts to an RDD
    # RDD of user rating data
    dataRDD = spark.read.text('hdfs:///user/maria_dev/ml-100k/u.data').rdd

    # trimming out columns we don't need
    cleanedRDD = dataRDD.map(parseLine)

    # If we're going to reuse this DF, we should call cache on it to make sure
    # the engine doesn't try to recreate the DF. Optimizes performance
    ratingDF = spark.createDataFrame(cleanedRDD).cache()

    # ALS collaborative filtering model
    # creating the model with hyper params
    als_model = ALS(maxIter=5,
                    regParam=0.01,
                    userCol="userID",
                    itemCol="movieID",
                    ratingCol="rating")

    # fitting the DataFrame with the model
    model = als_model.fit(ratingDF)
    
    # filter is like Where
    userRatings = ratingDF.filter("userID = 0")

    # checking added movies
    # test = userRatings.join(metaDF, ['movieID'], 'inner')
    
    # movies rated more than 100 times
    ratingCountDF = ratingDF.groupBy('movieID').count().filter('count > 100')

    # generating a DF to be used for testing our result
    # what is lit? -> literal
    # this is basically generating a 2 column DF by taking movieIDs from 
    # the ratingCountDF and creating a new column using WithColumn()
    testDF = ratingCountDF.select('movieID').withColumn('userID', lit(0))

    recs = model.transform(testDF)

    # grab top 20. ALSobj.prediction seem to give the prediction for each row
    # in the test DF
    topRecs = spark.createDataFrame(recs.sort(recs.prediction.desc()).take(20))

    rec_set = topRecs.join(metaDF, ['movieID'], 'inner')

    rec_set = rec_set.sort(desc('prediction'))

    rec_set.show()

    # stop session
    spark.stop()