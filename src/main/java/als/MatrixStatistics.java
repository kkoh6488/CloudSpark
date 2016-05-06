package als;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * This is a program to test some basic matrix and statistics
 * operations in Spark machine learning library.
 *
 * @author Ying Zhou
 */

public class MatrixStatistics{

    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        String inputDataPath = args[0];
        SparkConf conf = new SparkConf();
        conf.setAppName("Matrix Statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);
   
        // Create a coordinate matrix from the rating data

        JavaRDD<MatrixEntry> entries = 
            sc.textFile(inputDataPath+"ratings.csv").map(
                line -> {
                String[] data = line.split(",");
                return new MatrixEntry(Long.parseLong(data[0]),
                    Long.parseLong(data[1]),
                    Double.parseDouble(data[2]));
            });
        CoordinateMatrix mat = new CoordinateMatrix(entries.rdd());
        // Convert the rating data into RowMatrix
        // with the row representing user
        // column representing movie
        //
        RowMatrix ratings = mat.toRowMatrix();
        long numCols = ratings.numCols(),
             numRows = ratings.numRows();
        System.out.println("The row matrix has " + numRows + " rows and " + numCols + " columns");
        // Compute basic summary of movies
        //
        MultivariateStatisticalSummary summary = ratings.computeColumnSummaryStatistics();
        
        // Since the matrix is very sparse, the only useful statistics here is nonzeros
        // the number of none zeros indicate the popularity of movie
        //
        double[]  movieNonzeros = summary.numNonzeros().toArray();
        int i = 0;
        System.out.println("Movies with over 300 ratings are:");
        for (double nonZero: movieNonzeros){
            if (nonZero > 300)
                System.out.println( i + ":" + nonZero);
            i++;
        }          
    }
}
