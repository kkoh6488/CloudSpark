from pyspark import SparkContext
import re
import math


# This function convert entries of ratings.csv into (userid -> (movieid, rating))
def parse_rating(line):
	user_id, movie_id, rating, timestamp = line.strip().split(",")
	return (int(user_id.strip()), (int(movie_id.strip()), float(rating)))

# This function converts entries of movies.csv into (movieid -> title)
def parse_movie(line):
	fields = re.findall('("[^"]+"|[^,]+)', line.strip())
	return (int(fields[0]), fields[1])

### AVERAGE ###
# Calculate average
def calculate_average(line):
	ratings = line[1]
	count = 0
	total = 0
	for r in ratings:
		total += r[1]
		count += 1
	average = total/count
	return(line[0], (line[1], average))

### NEIGHBORHOOD SIM ###

# Set variables ACD for use
# userID -> ([(movieID, rating)],average)
def do_precalculation(i, line):
	ratings = dict(line[1][0])
	average = line[1][1]
	user_sim = []

	if i in ratings.keys():
		rating_i = ratings[i]
		a = rating_i - average
		c = a ** 2

		for j, rating_j in ratings.iteritems():
			b = rating_j - average
			d = b ** 2
			numerator = a * b
				
			# add to rdd
			temp_tup = ( (i,j) , (numerator,c,d) )
			user_sim.append(temp_tup)
	
	return tuple(user_sim)

# Final calcs
def add_topbottom(a, b):
	numa, ca, da = a
	numb, cb, db = b
	return(numa+numb, ca+cb, da+db)


def do_final_calc(line):
	num, c, d = line[1]
	c = math.sqrt(c)
	d = math.sqrt(d)
	final_calc = []
	if c*d != 0:
		final_calc.append((line[0][1], (line[0][0],num/(c*d))))
	return tuple(final_calc)

def keep_topten(line):
	top_ten = []
	data = line[1]
	top_ten= sorted(data, reverse=True, key=lambda tup: tup[1])[:10]
	return (line[0],top_ten)

### PREDICTION GENERATION ###
def calculate_predictions(line):
	num, denom = 0, 0
	for sim in line[1]:
		num += (sim[1] * p_rating[sim[0]])
		denom += abs(sim[1])
	return (num/denom, line[0])

if __name__ == "__main__":
	sc = SparkContext(appName="Workload2")

	# Format userID,MovieID,Rating,TimeStamp
	#personal_ratings = sc.textFile("/user/dzha9390/spark/personalRatings.txt")
	#ratings = sc.textFile("/share/movie/ratings.csv")
	# Format MovieID,Title,Genres
	#movies = sc.textFile("/share/movie/movies.csv")

	personal_ratings = sc.textFile("/Users/eddie/Desktop/personalRatings.txt")
	ratings = sc.textFile("/Users/eddie/Desktop/ratings.csv")
	movies = sc.textFile("/Users/eddie/Desktop/movies.csv")

	# userID -> (movieID, rating)
	entries = ratings.map(parse_rating)
	pentries = personal_ratings.map(parse_rating)

	# movieID -> title
	moventries = movies.map(parse_movie).collectAsMap()
	# Stores userID -> ([(movieID, rating)],average)
	user_aggregated = entries.groupByKey().map(calculate_average).cache()

	# Create personal rating dictionary
	p_rating = pentries.map(lambda entry:(entry[1][0],entry[1][1])).collectAsMap()

	# Get all ((i,j), variables)
	pre_calc_sim_rdd = sc.emptyRDD()
	for i in p_rating.keys():
		pre_calc_sim_rdd += user_aggregated.flatMap(lambda entry: do_precalculation(i, entry))
	
	# Aggregate by i,j and compute similarities then cut it off
	# Output i -> [(j, sim),(j, sim)]
	sim_ij = pre_calc_sim_rdd.reduceByKey(add_topbottom).flatMap(do_final_calc).groupByKey().map(keep_topten)
	
		# Calculate top predictions and only keep top 50
	top_predictions = sim_ij.map(calculate_predictions).sortByKey(ascending=False).take(50)
	#.sortBy(lambda entry: entry[1],  ascending=False)

	f=open('test.txt','w')
	for prediction in top_predictions:
		out = ""
		out = str(prediction[1]) + "," + moventries[prediction[1]] + ":" + str(prediction[0])
		f.write(out.encode('utf-8')+'\n') 
	f.close()