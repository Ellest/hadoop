/* Main dataset. Defining the schema and loading data from file */
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID: int, movieID:int, rating:int, ratingTime:int);

/* Metadata dataset. Loads from file using a pipe delimiter with the defined schema */
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);
   
/* Creating a set of movieID, movieTitle, and releaseTime to be joined later */
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

/* Dataset where all ratings are grouped under each movieID */
ratingsByMovie = GROUP ratings BY movieID;d

/* For each movie, create an entry (GENERATE) */
avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

/* Relation (dataset) that only includes movies with an average rating of above 4.0 */
fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

/* Joining metadata onto movie ratings by MovieID. NOTE: Joins alter column names*/
fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;

/* Sorting by release time*/
oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

/* This would be swapped with STORE in production */
DUMP oldestFiveStarMovies;