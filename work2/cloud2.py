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
# Merge count and sum values
def add_values(a, b):
	counta, sma = a
	countb, smb = b
	return (counta+countb, sma+smb)

# Calculate average
def calc_avg(line):
	uid, sm_count = line
	sm , count = sm_count
	return(uid, (sm/count))

### NEIGHBORHOOD SIM ###

# Set variables ACD for use
# userID -> ([(movieID, rating)],average)
def do_precalculation(line):
	average = line[1][1]
	user_ratings = dict(line[1][0])
	user_sim = []
	have_rated = [] 

	for pr in p_rating.keys():
		if pr in user_ratings:
			have_rated.append(pr)


	# don't calculate unless the user has rated a movie i
	if len(have_rated) != 0:
		for i in have_rated:
			rate_i = user_ratings[i]
			a = rate_i - average
			c = a ** 2

			for j in user_ratings.keys():
				if j != i:
					rate_j = user_ratings[j]
					b = rate_j - average
					d = b ** 2
					numerator = a * b
					# add to rdd
					temp_tup = ( (i,j) , (numerator,c,d) )
					user_sim.append(temp_tup)

		return tuple(user_sim)
	return ()

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
	return (line[0], num/denom)

if __name__ == "__main__":
	sc = SparkContext(appName="Workload2")

	
	# Format userID,MovieID,Rating,TimeStamp
	#personal_ratings = sc.textFile("/user/dzha9390/spark/personalRatings.txt")
	#ratings = sc.textFile("/share/movie/small/ratings.csv")
	# Format MovieID,Title,Genres
	#movies = sc.textFile("/share/movie/small/movies.csv")

	personal_ratings = sc.textFile("/Users/eddie/Desktop/personalRatings.txt")
	ratings = sc.textFile("/Users/eddie/Desktop/ratings.csv")
	movies = sc.textFile("/Users/eddie/Desktop/movies.csv")

	# userID -> (movieID, rating)
	entries = ratings.map(parse_rating)
	pentries = personal_ratings.map(parse_rating)

	# movieID -> title dictionary
	moventries = movies.map(parse_movie).collectAsMap()
	# Create personal rating dictionary
	p_rating = pentries.map(lambda entry:(entry[1][0],entry[1][1])).collectAsMap()

	# Average user rating
	user_rating_sum_count = entries.map(lambda entry: (entry[0],(entry[1][1], 1))).reduceByKey(add_values)
	user_rating_average = user_rating_sum_count.map(calc_avg)

	# Stores userID -> ([(movieID, rating)],average)
	user_aggregated = entries.groupByKey().join(user_rating_average).cache()
	
	# Get all ((i,j), variables)
	pre_calc_sim_rdd = user_aggregated.flatMap(do_precalculation)
	
	# Aggregate by i,j and compute similarities then cut it off
	# Output i -> [(j, sim),(j, sim)]
	sim_ij = pre_calc_sim_rdd.reduceByKey(add_topbottom).flatMap(do_final_calc).groupByKey().map(keep_topten)
	
	# Calculate top predictions and only keep top 50
	top_predictions = sim_ij.map(calculate_predictions).sortBy(lambda entry: entry[1],  ascending=False).take(50)


	f=open('test.txt','w')
	for prediction in top_predictions:
		out = ""
		out = str(prediction[0]) + "," + moventries[prediction[0]] + ":" + str(prediction[1])
		f.write(out.encode('utf-8')+'\n') 
	f.close()




