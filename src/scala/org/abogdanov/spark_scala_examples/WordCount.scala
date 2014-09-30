package org.abogdanov.spark_scala_examples

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
	def main(args: Array[String]) {

		// Check whether input arguments are present
		if (args.length < 2) {
			System.err.println("Usage: \n" +
				"./bin/spark-submit --class org.abogdanov.spark_scala_examples.WordCount " +
				"./<jarName> <inputFile> <outputDir>")
			System.exit(1)
		}

		// Create a Scala Spark Context
		val conf = new SparkConf().setMaster("local").setAppName("WordCountApp")
		val sc = new SparkContext(conf)

		// Load our input data
		val input = sc.textFile(args(0))
		// Split it up into words.
		val words = input.flatMap(line => line.split(" "))
		// Transform into word and count.
		val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(args(1))
	}
}
