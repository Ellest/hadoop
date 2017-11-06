from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatings(MRJob):

	def steps(self):
		return [
			MRStep(mapper=self.mapper,
				   reducer=self.reducer),
			# This chains a second reducer. The result from the MapReduce job from 
			# above will go through the "Shuffle and Sort" phase which will sort
			# result by the key (rating count). We can skipp the Mapper part as 
			# the result from the first job will not require a mapper to reformat.
			MRStep(reducer=self.reducer_sort)
		]

	def mapper(self, _, line):
		yield line.split('\t')[1], 1

	def reducer(self, key, values):
		# Since all values are strings in stdin and stdout, we need to cast 
		# the sum value as a string and zerio fill the string to ensure we
		# do a pseudo int comparison rather than a string comparison in the
		# sorting phase. Could use a json instead
		yield str(sum(values)).zfill(5), key

	def reducer_sort(self, cnt, movies):
		for movie in movies:
			yield movie, int(cnt)

if __name__ == '__main__':
	MovieRatings.run()