from pyspark import SparkContext
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
import collections
import numpy
import sys
import re
from os.path import join

from pyspark import SparkConf, SparkContext
from pyspark.mllib.linalg.distributed import *
from pyspark.mllib.stat import Statistics
from pyspark.sql import SQLContext

# This function convert entries of ratings.csv into a Matrix Entry
def extract_movrt(line):
    fields = re.findall('("[^"]+"|[^,]+)', line.strip())
    return MatrixEntry(int(fields[0]), int(fields[1]), float(fields[2]))

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


if __name__ == "__main__":
	sc = SparkContext(appName="Workload2")
	sqlContext = SQLContext(sc)
	sqlCtx = sqlContext

	# Format userID,MovieID,Rating,TimeStamp
	#personal_ratings = sc.textFile("/user/dzha9390/spark/personalRatings.txt")
	#ratings = sc.textFile("/share/movie/small/ratings.csv")
	# Format MovieID,Title,Genres
	#movies = sc.textFile("/share/movie/small/movies.csv")

	personal_ratings = sc.textFile("/Users/eddie/Desktop/personalRatings.txt")
	ratings = sc.textFile("/Users/eddie/Desktop/ratings.csv")
	movies = sc.textFile("/Users/eddie/Desktop/movies.csv")

	# Create movie sim matrix
	# w, h = max(unrated_movies.keys()), len(have_rated)
	# matrix = [[-1 for x in range(w)] for y in range(h)]

	entries = ratings.map(extract_movrt)
	mat = CoordinateMatrix(entries)
	matrix = mat.entries.cache()

	# Average user rating
	user_rating_sum_count = matrix.map(lambda entry: (entry.i, (1, entry.value))).reduceByKey(add_values)
	user_rating_average = user_rating_sum_count.map(calc_avg)
	
	
	user_rating_average.saveAsTextFile("test")


