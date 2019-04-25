// Spark implementation of job to count the number of times each
// unique IP address 4-tuple appears in an adudump file.
//

import scala.Tuple2;

import org.apache.spark.SparkContext.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;


import java.util.*;
import java.io.*;
import java.text.*;

public final class AverageCountryTempByYear {

    // The first argument to the main function is the HDFS input file name
    // (specified as a parameter to the spark-submit command).  The
    // second argument is the HDFS output directory name (must not exist
    // when the program is run).

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: AverageCountryTempByYear <input file> <output file>");
            System.exit(1);
        }

        // Create session context for executing the job
        SparkSession spark = SparkSession
                .builder()
                .appName("AverageCountryTempByYear")
                .getOrCreate();

        // Create a JavaRDD of strings; each string is a line read from
        // a text file.
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();


        // The flatMap operation applies the provided function to each
        // element in the input RDD and produces a new RDD consisting of
        // the strings returned from each invocation of the function.
        // The strings are returned to flatMap as an iterator over a
        // list of strings (string array to string list with iterator).
        //
        // The RDD 'words' will have an entry for each IP address that
        // appears in the input HDFS file (most addresses will appear
        // more than once and be repeated in 'words').

        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) {
                        String[] tokens = s.split(" ");
                        String date = new String();
                        String year = new String();
                        String averageTemp = new String();

                        int first_hyphen;

                        // get year
                        date = tokens[0];
                        first_hyphen = date.indexOf('-');
                        year = date.substring(0, first_hyphen);

                        averageTemp = tokens[1];

                        String[] year_temps = {year + " " + averageTemp};

                        return Arrays.asList(year_temps).iterator();
                    }
                }
        );


        //Create a PairRDD of <Key, Value> pairs from an RDD.  The input RDD
        //contains strings and the output pairs are <String, Integer>.
        //The Tuple2 object is used to return the pair.  mapToPair applies
        //the provided function to each element in the input RDD.
        //The PairFunction will be called for each string element (IP address)
        //in the 'words' RDD.  It will return the IP address as the first
        //element (key) and 1 for the second (value).

        JavaPairRDD<String, Double> ones = words.mapToPair(
                new PairFunction<String, String, Double>() {
                    public Tuple2<String, Double> call(String s) {
                        String[] tokens = s.split(" ");
                        return new Tuple2<>(tokens[0], Double.parseDouble(tokens[1])); //key = year, value = temp
                    }
                });

        //Create a PairRDD where each element is one of the keys from a PairRDD and
        //a value which results from invoking the supplied function on all the
        //values that have the same key.  In this case, the value returned
        //from the jth invocation is given as an input parameter to the j+1
        //invocation so a cumulative value is produced.

        //The Function2 reducer is similar to the reducer in Mapreduce.
        //It is called for each value associated with the same key.
        //The two inputs are the value from the (K,V) pair in the RDD
        //and the result from the previous invocation (0 for the first).
        //The sum is stored in the result PairRDD with the associated key.

        JavaPairRDD<String, Double> counts = ones.reduceByKey(
                new Function2<Double, Double, Double>() {
                    public Double call(Double d1, Double d2) {
                        return d1 + d2;
                    }
                });

        //Format the 'counts' PairRDD and write to a HDFS output directory
        //Because parts of the RDD may exist on different machines, there will
        //usually be more than one file in the directory (as in MapReduce).
        //Use 'hdfs dfs -getmerge' as before.

        counts.saveAsTextFile(args[1]);

        spark.stop();
    }
}