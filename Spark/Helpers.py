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