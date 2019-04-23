/** @author Jake Mathura
* I certify that no unauthorized assistance has been received or given in the completion of this work
**/

import scala.Tuple2;

import org.apache.spark.SparkContext.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;


import java.util.*;
import java.io.*;
import java.text.*;

public final class OverallMaxLandTemp {

    // The first argument to the main function is the HDFS input file name
    // (specified as a parameter to the spark-submit command).  The
    // second argument is the HDFS output directory name (must not exist
    // when the program is run).

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: OverallMaxLandTemp <input file> <output file>");
            System.exit(1);
        }

        // Create session context for executing the job
        SparkSession spark = SparkSession
                .builder()
                .appName("OverallMaxLandTemp")
                .getOrCreate();

        // Create a JavaRDD of strings; each string is a line read from
        // a text file.
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) {
                        String[] tokens = s.split("\\s+");

                        String date = new String();
                        String year = new String();
                        double maxLandTemp = Integer.MIN_VALUE;
                        int first_hyphen, first_slash;

                        // get year
                        date = tokens[0];

                        if(date.length() == 10) {
                            first_hyphen = date.indexOf('-');
                            year = date.substring(0, first_hyphen);
                        } else {
                            first_slash = date.lastIndexOf('/');

                            if(tokens.length == 10) {
                                year = "20" + date.substring(first_slash + 1, date.length());
                            } else {
                                year = "19" + date.substring(first_slash + 1, date.length());
                            }
                        }

                        try {
                            maxLandTemp = Double.parseDouble(tokens[3]);
                        } catch (Exception e) {
                        }

                        String[] yearAndTemp = {year + " " + maxLandTemp};

                        return Arrays.asList(yearAndTemp).iterator();
                    }
                }
        );

        JavaPairRDD<String, Double> temps = words.mapToPair(
                new PairFunction<String, String, Double>() {
                    public Tuple2<String, Double> call(String s) {
                        String[] tokens = s.split(" ");

                        return new Tuple2<>(tokens[0], Double.parseDouble(tokens[1])); //key = year, value = temp
                    }
                });

        JavaPairRDD<String, Double> mins = temps.reduceByKey(
                new Function2<Double, Double, Double>() {
                    public Double call(Double d1, Double d2) {
                        if(d1 > d2) {
                            return d1;
                        } else {
                            return d2;
                        }
                    }
                });

        mins.saveAsTextFile(args[1]);

        spark.stop();
    }
}
