from pyspark import SparkConf, SparkContext
from Helpers import loadMovieNames, parseInput

if __name__ == '__main__':
	# creating a Spark Configuration object by passing the config name to the constructor
	conf = SparkConf().setAppName("WorstMovies")
	# creating a SparkContext
	sc = SparkContext(conf = conf)

	# loading up movies
	# Doing this locally as the current dataset is small enough to handle locally
	movieNames = loadMovieNames('ml-100k/u.item')

	# loading a raw u.data file into an RDD
	lines = sc.textFile('hdfs://user/maria_dev/ml-100k/u.data')

	# Creating an RDD with a key-value pair of:
	# 	(movieID, (rating, 1.0))
	# Using a predefined function "parseInput" for the map function
	movieRatings = lines.map(parseInput)

	# reduceByKey requires a function with 2 parameters
	# The passed in function combines 2 given values for a given key
	# IMPORTANT NOTE:
	#	Eventhough RDD's are represented as tuples (a,b,(c,d),...), the first
	#	value is the KEY. Thus m1[0] below actually is the 'rating' from (rating, 1.0)
	#	Thus the below operation is summing up the ratings
	# Reduce to (movieID, (sumOfRatings, totalRatings))
	sumAndTotal = movieRatings.reduceByKey(lambda m1, m2: (m1[0] + m2[0], m1[1] + m2[1]))

	# mapping key to sumRating / ratingCount -> (movieID, averageRating)
	avgPerMovie = sumAndTotal.mapValues(lambda m: m[0] / m[1])

	# does mapping (map, mapValues) force a key-value pair?

	# sort by average rating
	sortedMovies = avgPerMovie.sortBy(lambda m: m[1])

	# top 10
	result_set = sortedMovies.take(10)

	for id, avg in result_set:
		print(movieNames[id], avg)



