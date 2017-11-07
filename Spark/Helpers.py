def loadMovieNames(file_path):
	"""
	Loads a dictionary (HashMap) of ID-name key-value pairs from the given
	input file and returns the dictionary. 
	"""
	movieNames = {}
	with open(file_path) as f:
		for line in f:
			cols = line.split('|')
			movieNames[int(cols[0])] = cols[1]
	return movieNames

def parseInput(line):
	"""
	Funciton that parses through a line and returns a tuple in the form of
	(movieID, (rating, 1.0)).
	Args:
		line: line to be parsed
	"""
	fields = line.split()
	# (movieID, (rating, 1.0)). second subelem of second element is to keep track
	# of the count
	return (int(fields[1]), (float(fields[2]), 1.0))