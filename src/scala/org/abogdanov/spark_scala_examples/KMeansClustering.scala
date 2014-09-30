package org.abogdanov.spark_scala_examples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


object KMeansClustering {
	def main(args: Array[String]) {

		// Check whether input arguments are present
		if (args.length < 2) {
			System.err.println("Usage: \n" +
				"./bin/spark-submit --class org.abogdanov.spark_scala_examples.KMeansClustering " +
				"./<jarName> <inputFile> <outputDir>")
			System.exit(1)
		}

		// Create a Scala Spark Context
		val conf = new SparkConf().setMaster("local").setAppName("KMeansClusteringApp")
		val sc = new SparkContext(conf)

		// Load and parse the data
		val data = sc.textFile(args(0))
		val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

		// Cluster the data into two classes using KMeans
		val numClusters = 2
		val numIterations = 20
		val clusters = KMeans.train(parsedData, numClusters, numIterations)

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		val WSSSE = clusters.computeCost(parsedData)
		println("Within Set Sum of Squared Errors = " + WSSSE)
	}
}
