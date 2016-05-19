from pyspark import SparkContext

### Calculate Average ###
# This function convert entries of ratings.csv into key,value pair of the following format
# user_id -> 1
def extract_rtc(record):
	try:
		user_id, movie_id, rating, timestamp = record.strip().split(",")
		return (int(user_id.strip()), 1)
	except:
		pass

# This function convert entries of ratings.csv into key,value pair of the following format
# user_id -> rating
def extract_rt(record):
	try:
		user_id, movie_id, rating, timestamp = record.strip().split(",")
		rating = float(rating)
		return (int(user_id.strip()), rating)
	except:
		pass

# This function is used by reduceByKey function to merge count of the same key
# This functions takes in two values - merged count from previous call of sum_rating_count, and the currently processed count
def sum_rating_count(reduced_count, current_count):
	return reduced_count+current_count

# This functions convert tuples of ((user_id, sum_of_rating), number of rating) into key,value pair of the following format
# user_id -> average_rating
def map_average_to_pair(record):
	uid, usum_count = record
	usum, count = usum_count
	average = usum / count
	return (uid, average)

### Calculate Neighbourhood Similarity ###
# This function convert entries of movies.csv into key,value pair of the following format whilst also 
# movie_id -> title
def extract_id(record):
	try:
		movie_id, title, genres = record.strip().split(",")
		movie_id = int(movie_id)
		if (movie_id not in have_rated.keys()):
			return (movie_id, title)
	except:
		pass

# This function convert entries of personal_ratings.csv into key,value pair of the following format
# movie_id -> rating
def extract_prt(record):
	try:
		user_id, movie_id, rating, timestamp = record.strip().split(",")
		rating = float(rating)
		return (int(movie_id), rating)
	except:
		pass




if __name__ == "__main__":
	sc = SparkContext(appName="Workload2")

	# Format userID,MovieID,Rating,TimeStamp
	# personal_ratings = sc.textFile("/user/dzha9390/spark/personalRatings.txt")
	# ratings = sc.textFile("/share/movie/small/ratings.csv")
	# Format MovieID,Title,Genres
	# movies = sc.textFile("/share/movie/small/movies.csv")

	personal_ratings = sc.textFile("/Users/eddie/Desktop/personalRatings.txt")
	ratings = sc.textFile("/Users/eddie/Desktop/ratings.csv")
	movies = sc.textFile("/Users/eddie/Desktop/movies.csv")

	# Contains user_id -> number_of_ratings
	user_ratings_count = ratings.map(extract_rtc).reduceByKey(sum_rating_count)
	# Contains user_id -> sum_of_rating
	user_sum_ratings = ratings.map(extract_rt).reduceByKey(sum_rating_count)
	
	# Contains user_id -> user_id averating rating
	user_rating_average = user_sum_ratings.join(user_ratings_count).map(map_average_to_pair)
	# Stored as dictionary for easy lookup
	user_average = user_rating_average.collectAsMap()

	# Create set of movies I rated: movie_id -> rating
	have_rated = personal_ratings.map(extract_prt).collectAsMap()
	# Find movies that the user hasn't rated
	unrated_movies = movies.map(extract_id)

	# Create movie sim matrix
	w, h = 15, 149532 
	matrix = [[-1 for x in range(w)] for y in range(h)] 

	#for i in have_rated.keys():
		#for k in unrated_movies.keys():
			#matrix[i][j] = sim(i,j)

