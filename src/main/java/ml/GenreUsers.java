package ml;

import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;

import scala.Tuple2;

//import scala.*;
//import scala.Double;

/**
 * Run  on latest movie lens data to find out
 * the average rating for each genre.
 *
 *
 * input data :
 * movies.csv (only 1.33MB)
 *
 * format
 * 	movieId,title,genres.
 * sample data
 * 	1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
 *  102604,"Jeffrey Dahmer Files, The (2012)",Crime|Documentary
 *
 * Genres are a pipe-separated list;
 * Movie titles with comma is enclosed by a pair of quotes.
 *
 * ratings.csv (541.96MB)
 *
 * format
 *   userId,movieId,rating,timestamp

 *sample data
 * 1,253,3.0,900660748
 *
 *
 *submit to a yarn cluster
 *
 *spark-submit  \
 --class ml.MovieLensLarge \
 --master yarn-cluster \
 sparkML.jar \
 hdfs://ip-10-171-118-84.ec2.internal:8020/share/ml/latest/ \
 hdfs://ip-10-171-118-84.ec2.internal:8020/user/ying/spark/
 *
 *
 * @author zhouy
 *
 */
public class GenreUsers {

    public static void main(String[] args) {

        //The program arguments are input and output path
        //The path should be absolute path
        //For windows system, the path value should be something like "C:\\data\\ml-100k\\"
        //For unix system, the path value should something like "/home/user1/data/ml-100k/"
        //For HDFS, the path value should be something like "hdfs://ip-10-171-118-84.ec2.internal:8020/share/ml/100k/"

        String inputDataPath = args[0], outputDataPath = args[1];
        SparkConf conf = new SparkConf();

        conf.setAppName("Movie Lens Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings.csv"),
                movieData = sc.textFile(inputDataPath + "movies.csv");

        //read ratings.csv and get the total number of movies rated and avg rating for each user
        // 1. Read ratings.csv and produce records with relevant data
        // Produces movieID, <userID, rating> pairs.
        JavaPairRDD<String, Tuple2<String, Float>> userRatings = ratingData.mapToPair(s ->
                {  String[] values = s.split(",");
                    return
                            new Tuple2(values[1], new Tuple2(values[0], Float.parseFloat(values[2])));
                }
        );

        // 2. Read movies table and generate movieID, <genre>
        JavaPairRDD<String, Tuple2<String, Float>> movieGenres = movieData.mapToPair(s ->
                {  String[] values = s.split(",");
                    return
                            new Tuple2(values[1], new Tuple2(values[0], Float.parseFloat(values[2])));
                }
        );

        // 3. Join with the movies table

        JavaPairRDD<String, >

        // Get the total number of ratings per user
        //reduceByKey((n1,n2) -> n1+ n2);
        //JavaPairRDD<String, Integer> ratingsPerUser = userRatings.reduceByKey((sum, rating) -> (1));
        //JavaPairRDD<String, Integer> ratingsPerUser = userRatings.reduceByKey()

        // Reduce function for summing ratings per user
        // Function2<t1, t2, r>
        // Defines a function taking 2 arguments of type t1 and t2, returning type r
        // total and n are the 2 arguments for each row, then they are added to produce a running total
        //Function2<Float, Float, Float> sumRatings = (total, n) -> (total + n);

        // 1. Define a function that reduces rows from userRatings into a single userId, ratingSum, numRatings row
        // We use the t1._1 and t2._1 notation to specify the 'column' of the tuple we are accessing.

        Function2<Tuple2<Float, Integer>, Tuple2<Float,Integer>, Tuple2<Float, Integer>> userTotals =
                (t1, t2) -> new Tuple2<Float, Integer>(t1._1 + t2._1, t1._2 + t2._2);

        JavaPairRDD<String, Tuple2<Float, Integer>> ratingSums = userRatings.reduceByKey(userTotals);

        ratingSums.saveAsTextFile(outputDataPath);
        sc.close();

        // 2. Use map to produce output rows of <UserId, avgRating, numRatings>

        // 3. Join with the original dataset to get <userId, movieId,

        // Join results into 1 row for each userId, <ratingSum, numRated>

        //(r,v)-> new Tuple2<Float, Integer> (r._1+ v, r._2+1),
        //        (v1,v2) -> new Tuple2<Float,Integer> (v1._1 + v2._1, v1._2 + v2._2))
        /*JavaPairRDD<String, Tuple2<Float, Integer>> totals = userRatings.aggregateByKey(
                0,
                1,
                (rating, count) -> (rating, )
                )
        */

        //JavaPairRDD<String, Tuple2<Float, Float>> totals = ratingsPerUser.join(ratingSums);

        //ratingsPerUsersaveAsTextFile(outputDataPath + "totalPerUser");

        //read movies.csv and convert it to a key value pair RDD of the following format
        //movieID, genre
        //flatMapToPair is used because one movie can have multiple genres
        /*

        JavaPairRDD<String,String> movieGenres = movieData.flatMapToPair(s->{
            String[] values = s.split(",");
            String movieID = values[0];
            int length = values.length;
            ArrayList<Tuple2<String,String>> results = new ArrayList<Tuple2<String,String>>();
            if (values.length >=3 ){ // genre list is present
                String[] genres = values[length -1].split("\\|"); //genres string is always at the last index
                for (String genre: genres){
                    results.add(new Tuple2<String, String>(movieID, genre));
                }
            }
            return results;
        });


        //join the two RDDs to find the ratings for each genre
        //join function performs an inner join
        //The result RDD would have the following format
        //(movieID, (genre, rating))

        JavaPairRDD<String, Tuple2<String,Float>> joinResults = movieGenres.join(ratingExtraction);

        // System.out.println("There are " + joinResults.count() + " rows after the join.");
        //Join is based on movieID, which is not useful in our calculation
        //We only want to retain the value which is (genre, rating) and convert it to a PairRDD
        JavaPairRDD<String, Float> joinResultsNoID = joinResults.values().mapToPair(v->v);



        //aggregateByKey operation takes one zero value and two functions:
        //mergeValue() and mergeCombiner()

        //mergeValue() function is applied on the given zero and any value belonging to a same key to get a partial result.
        //Since each partition is processed independently, we can have multiple partial results for the same key.
        //mergeCombiner() function is used to merge partial results.
        //we only want to have one partition for the result RDD, because the number of key is really small
        //output of aggregateByKey is of format:
        //(genre,<totalRating,NumOfRating>)

        //The mapToPair operation will calculate the average for each genre
        //the input of the mapToPair is of the format
        //<genreID, <totalRating, numOfRating>>
        //the mapTopair will covert the value to totalRating/numOfRating

        JavaPairRDD genreRatingAvg = joinResultsNoID.aggregateByKey(
                new Tuple2<Float, Integer> (0.0f,0),
                1,
                (r,v)-> new Tuple2<Float, Integer> (r._1+ v, r._2+1),
                (v1,v2) -> new Tuple2<Float,Integer> (v1._1 + v2._1, v1._2 + v2._2))
                .mapToPair(
                        t -> new Tuple2(t._1, (t._2._1 * 1.0 / t._2._2))
                );

        // this is an action

        genreRatingAvg.saveAsTextFile(outputDataPath + "latest.rating.avg.per.genre");
        sc.close();
        */
    }
}
