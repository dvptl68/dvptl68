package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.*;

public final class App {
    private App() {
    }

    private static String delimiters = "[\\s-\\t,;.?!:@\\[\\](){}_*/]";

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {

        /* Spark initialization */
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");

        /* Get input lines from file */
        JavaRDD<String> inputLines = sc.textFile("declaration.txt");

        /* Split lines into individual words */
        JavaRDD<String> words = inputLines.flatMap(string -> Arrays.asList(string.split(delimiters)).iterator()).filter(word -> !word.isEmpty());

        /* Create pair map with word counts */
        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((x, y) -> x + y);

        /* Find maximum occurence word(s) */
        int max = counts.values().reduce((x, y) -> (x > y) ? x : y);
        JavaRDD<String> maxWords = counts.filter(x -> x._2.equals(max)).keys();

        System.out.println("\nMax frequency word(s): ");
        maxWords.foreach(word -> System.out.println(word));

        sc.close();
    }
}
