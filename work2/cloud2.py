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
def filter_rated(line):
	if line[0] not in have_rated:
		return True
	return False

def build_user(line):
	ratings = line[1][0]
	bi, bj = False, False
	for r in ratings:
		if int(r[0]) == i:
			bi = True
		if int(r[0]) == j:
			bj = True
	if bi and bj:
		return True



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
	havent_rated = moventries.filter(filter_rated).collectAsMap()

	for i in have_rated:
		for j in havent_rated.keys():
			sim_rdd = user_aggregated.filter(build_user)
			
			print(sim_rdd.collectAsMap().keys())
			print(i)
			print(j)

