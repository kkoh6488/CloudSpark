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
def do_precalculation(i, line):
	ratings = line[1][0]
	average = line[1][1]
	user_sim = []
	ratei = -1 

	# if this user has rated i
	for r in ratings:
		if r[0] == i:
			ratei = r[1]

	# don't calculate unless the user has rated i
	if ratei != -1:
		# calcate A and C
		a = ratei - average
		c = a ** 2
		for r in ratings:
			j = r[0]
			
			# don't calculate sim(i,i)
			if j != i:
				b = r[1] - average
				d = b ** 2
				numerator = a * b
				
				# add to rdd
				temp_tup = ( (i,j) , (numerator,c,d) )
				user_sim.append(temp_tup)
		return tuple(user_sim)
	return ()

# Final calculations
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

	# movieID -> title
	moventries = movies.map(parse_movie)

	# Average user rating
	user_rating_sum_count = entries.map(lambda entry: (entry[0],(entry[1][1], 1))).reduceByKey(add_values)
	user_rating_average = user_rating_sum_count.map(calc_avg)


	# Stores userID -> ([(movieID, rating)],average)
	user_aggregated = entries.groupByKey().join(user_rating_average).cache()

	# Create set of movies
	have_rated = pentries.map(lambda entry: entry[1][0]).collect()

	# Get all ((i,j), variables)
	pre_calc_sim_rdd = sc.emptyRDD()
	for i in have_rated:
		pre_calc_sim_rdd += user_aggregated.flatMap(lambda entry: do_precalculation(i, entry))
	
	# Aggregate by i,j and compute
	sim_ij = pre_calc_sim_rdd.reduceByKey(add_topbottom).flatMap(do_final_calc).take(10)
	
	#print(sim_ij.collect())
	sim_ij.saveAsTextFile("test")




