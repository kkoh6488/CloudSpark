package ml;

import java.util.*;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Finds the top users per genre, including:
 * - Total number of ratings in the whole dataset
 * - Average rating for the whole dataset
 * - Total number of ratings for the genre they topped
 * - Average rating for the genre they topped
 */
public class SumRatings {

    public static void main(String[] args) {

        String inputDataPath = args[0];
        SparkConf conf = new SparkConf();
        conf.setAppName("Movie Lens Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings.csv").cache(),
                movieData = sc.textFile(inputDataPath + "movies.csv");

        // Part 1: Read ratings.csv and get the total number of movies rated and avg rating for each user
        // First, read ratings.csv and produce records with relevant data
        // Output: userId, rating
        JavaPairRDD<String, Tuple2<Float, Integer>> userRatings = ratingData.mapToPair(s ->
                {  String[] values = s.split(",");
                    return
                            new Tuple2(values[0], new Tuple2(Float.parseFloat(values[2]), 1));
                }
        );

        // Define a function that reduces rows from userRatings into a single userId, ratingSum, numRatings row
        Function2<Tuple2<Float, Integer>, Tuple2<Float,Integer>, Tuple2<Float, Integer>> userTotals =
                (t1, t2) -> new Tuple2(t1._1 + t2._1, t1._2 + t2._2);

        // Output: userId, <ratingSum, numRatings>
        JavaPairRDD<String, Tuple2<Float, Integer>> ratingSums = userRatings.reduceByKey(userTotals);

        // Format sums as: userId, avgRating \t totalRatings
        // So it can be joined later
        JavaPairRDD<String, String> formattedSums = ratingSums.mapToPair(s -> new Tuple2(s._1, (s._2._1 / (float) s._2._2) + "\t" + s._2._2));

        // Part 2: Get top 5 users per genre
        // Output: <movieId, userId \t rating>
        JavaPairRDD<String, String> movieRatings = ratingData.mapToPair(s ->
                {  String[] values = s.split(",");
                    return
                            new Tuple2(values[1], values[0] + "\t" + Float.parseFloat(values[2]));
                }
        ).cache();

        // Get movie genres
        // Output: <movieId, genresString>
        JavaPairRDD<String, String> movieGenres = movieData.mapToPair(s ->
            {
                // Can't split by comma, as some movies have titles with commas!
                String[] values = s.split(",");
                String genres;
                // If there are commas in the title
                if (values.length > 3) {
                    int commaIndex = s.lastIndexOf(",");
                    genres = s.substring(commaIndex + 1);
                }
                else {
                    genres = values[2];
                }
                return new Tuple2(values[0], genres);
            }
        );

        // Join user ratings to movie genres by movieId
        // Output: movieId, <genres, userId \t rating>
        JavaPairRDD<String, Tuple2<String, String>> join = movieGenres.join(movieRatings);

        // Map - produce <genre, userId> rows and count to get top 10 per genre. Each row represents a movie a user has
        // rated in the genre.
        // Input: movieId, <genres, userId \t rating>
        // Output: genre \t userId, GenreCount (genre, userId, 1, rating)
        JavaPairRDD<String, GenreCount> genreUsers = join.values().flatMapToPair(v->{
            ArrayList<Tuple2<String, GenreCount>> results = new ArrayList();

            // Get the user Id and separate each genre - emit a row for each genre and userId pair
            int tabIndex = v._2.indexOf('\t');
            String userId = v._2.substring(0, tabIndex);
            float rating = Float.parseFloat(v._2.substring(tabIndex));
            String genreList = v._1;
            String[] genres = genreList.split("\\|");
            for (String g : genres) {
                results.add(new Tuple2(g + "\t" + userId, new GenreCount(g, userId, 1, rating)));
            }
            return results;
            }
        );

        // Reduce counts - get number of ratings and sum of ratings a user has in each genre, then map it back to genre, GenreCount
        // instead of genre \t userId, GenreCount (ie change the key so its only genre).
        // Output: genre, GenreCount
        JavaPairRDD<String, GenreCount> genreRatingsPerUser = genreUsers.reduceByKey(
                (g1, g2) -> new GenreCount(g1.genre, g1.userId, g1.count + g2.count, g1.rating + g2.rating)
        ).mapToPair(v -> new Tuple2(v._2.genre, v._2));

        // Group by genre, so we get all the ratings by genre
        JavaPairRDD<String, Iterable<GenreCount>> totalGenreCounts = genreRatingsPerUser.groupByKey(1);

        // Get the top 5 users per genre - sort GenreCounts and output top 5.
        // Order is determined by the comparator the GenreCounts object.
        // Output: userId, GenreCount
        JavaPairRDD<String, GenreCount> topUsers = totalGenreCounts.flatMapToPair(v ->
            {
                TreeSet<GenreCount> counts = new TreeSet();
                for (GenreCount g : v._2) {
                    counts.add(g);
                }

                ArrayList<Tuple2<String, GenreCount>> result = new ArrayList();
                for (int i = 0; i < 5; i++) {
                    if (counts.isEmpty()) {
                        break;
                    }
                    GenreCount g = counts.pollFirst();
                    result.add(new Tuple2(g.userId, g));
                }
                return result;
            }
        );

        // Join top users with job 1 data to get stats for total dataset
        // Print the final result - GenreKey (toString) \t avgRating \t numRatings
        JavaPairRDD<String, Tuple2<GenreCount, String>> totalStatJoin = topUsers.join(formattedSums);
        JavaRDD<String> finalResult = totalStatJoin.map(s -> s._2._1.toResultString() + s._2._2);

        finalResult.saveAsTextFile("debug/final.txt");

        sc.close();
    }
}
