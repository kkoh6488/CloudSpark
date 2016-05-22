from pyspark import SparkContext
import re

# This function convert entries of ratings.csv into (userid -> (movieid, rating))
def parse_rating(line):
	fields = re.findall('("[^"]+"|[^,]+)', line.strip())
	return (int(fields[0]), (int(fields[1]), float(fields[2])))

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
def do_precalculation(line):
	if line is not None:
		ratings = line[1][0]
		average = line[1][1]
		usersims = []
		irating = -1 
		# if this user has rated i
		for r in ratings:
			if r[0] == i:
				irating = r[0]

		# don't calculate unless the user has rated i
		if irating != -1:
			# calcate A and C
			a = irating - average
			c = a ** 2
			for r in ratings:
				# don't calculate sim(i,i)
				if r[0] != i:
					c = r[0] - average
					d = c ** 2
					# add to rdd
					temp_tup = ((i,r[0]),(a,b,c,d))
					usersims.append(temp_tup)
			return usersims


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
	# havent_rated = moventries.filter(filter_rated).collectAsMap()

	sim_rdd = sc.parallelize(((0,0),(0,0,0,0)))
	for i in have_rated:
		print(user_aggregated.flatMap(do_precalculation).saveAsTextFile("test"))

	
