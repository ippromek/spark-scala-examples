package org.abogdanov.spark_scala_examples

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingCount {
	def main(args: Array[String]) {

		// Check whether input arguments are present
		if (args.length < 2) {
			System.err.println("Usage: \n" +
				"./bin/spark-submit --class org.abogdanov.spark_scala_examples.StreamingCount ./<jarName> <hostname> <port>")
			System.exit(1)
		}

		// Create a Scala Spark Context
		val sparkConf = new SparkConf().setAppName("StreamingCountApp")
		// Create a StreamingContext with a SparkConf configuration
		val ssc = new StreamingContext(sparkConf, Seconds(1))

		// Create a socket stream on target ip:port and count the
		// words in input stream of \n delimited text (eg. generated by 'nc')
		// Note that no duplication in storage level only for running locally.
		// Replication necessary in distributed scenario for fault tolerance.
		val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

		val words = lines.flatMap(_.split(" "))
		val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

		wordCounts.print()
		ssc.start()
		ssc.awaitTermination()
	}
}