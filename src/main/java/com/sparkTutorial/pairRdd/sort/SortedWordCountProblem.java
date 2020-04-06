package com.sparkTutorial.pairRdd.sort;


import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        JavaSparkContext context = new JavaSparkContext(
            new SparkConf()
                .setAppName("sorted word count")
                .setMaster("local[*]")
        );

        JavaPairRDD<Integer, String> words =
            context.textFile("in/word_count.text")
                   .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                   .mapToPair(token -> new Tuple2<>(token, 1))
                   .reduceByKey(Integer::sum)
                   .mapToPair(Tuple2::swap)
                   .sortByKey(false);

        for (Tuple2<Integer, String> word : words.collect()) {
            System.out.println(word._2() + " : " + word._1());
        }
    }
}

