from pyspark.sql import SparkSession, Row, functions, types
from Helpers import loadMovieNames
import json

def parseRating(line):
	cols = line.split()
	return Row(movieID = int(cols[1]), rating = float(cols[2]))

def movieNameDF(file_path):
	with open(file_path) as f:
		for l in f:
			

"""
TODO:

	Implement with struct (types)

"""

if __name__ =='__main__':

	# Create or get the SparkSession if it exists
	# SparkSession -> encompasses both a SparkContext & SQLContext
	# Leave the session running through the script
	#	similar to tfsession() in tensorFlow
	spark = SparkSession.builder.appName('Pop').getOrCreate()
	
	# load movie names using the u.item file. Defined in helpers module
	movieNames = loadMovieNames('ml-100k/u.item')

	# import movie data from u.data on HDFS
	data = spark.sparkContext.textFile('hdfs:///user/maria_dev/ml-100k/u.data')

	# convert to RDD
	movies = data.map(parseRating)

	# Better to use collect() to bring the RDD contents back to the driver, because 
	# foreach executes on the worker nodes and the outputs may not necessarily 
	# appear in your driver / shell (it probably will in local mode, but not when
	# running on a cluster
	#for m in movies.collect():
	#	print(m)

	# creating a data frame out of the RDD
	moviesDataset = spark.createDataFrame(movies)

	# These fields are the column names in the HDFS 
	# This was defined when the data file was uploaded to HDFS using Ambari
	# Think of this as a table that exists on the HDFS
	movieAverage = moviesDataset.groupBy("movieID").avg("rating")

	voteCount = moviesDataset.groupBy("movieID").count()

	merged = movieAverage.join(voteCount, "movieID")

	# Pay attention to the name schema for aggregated columns
	topTen = merged.orderBy("avg(rating)").take(10)

	nameDF = movieNameDF('ml-100k/u.item')
	#for rowObj in topTen:
		



