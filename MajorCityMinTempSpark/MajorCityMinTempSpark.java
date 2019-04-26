import scala.Tuple2;

import org.apache.spark.SparkContext.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;


import java.util.*;
import java.io.*;
import java.text.*;

public final class MajorCityMinTempSpark {

    // The first argument to the main function is the HDFS input file name
    // (specified as a parameter to the spark-submit command).  The
    // second argument is the HDFS output directory name (must not exist
    // when the program is run).

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: MajorCityMinTempSpark <input file> <output file>");
            System.exit(1);
        }

        // Create session context for executing the job
        SparkSession spark = SparkSession
                .builder()
                .appName("MajorCityMinTempSpark")
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
                        String city = tokens[3];
                        double maxCityTemp = Integer.MAX_VALUE;
                        try {
                            maxCityTemp = Double.parseDouble(tokens[1]);
                        } catch (Exception e) {
                        }

                        String[] yearAndTemp = {city + " " + maxCityTemp};

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
                        if(d1 < d2) {
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
