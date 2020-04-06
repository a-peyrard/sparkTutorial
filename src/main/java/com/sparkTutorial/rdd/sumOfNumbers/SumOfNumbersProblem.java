package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.OFF);

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        JavaSparkContext context = new JavaSparkContext(
            new SparkConf()
                .setAppName("100 first prime numbers sum")
                .setMaster("local[2]")
        );

        Double count = context.textFile("in/prime_nums.text")
                              .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                              .filter(s -> !s.isEmpty())
                              .mapToDouble(Double::parseDouble)
                              .reduce(Double::sum);
        System.out.println("count = " + count);
    }
}
