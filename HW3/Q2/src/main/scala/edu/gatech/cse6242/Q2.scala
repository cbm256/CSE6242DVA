package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Q2 {

	def main(args: Array[String]) {
    	val sc = new SparkContext(new SparkConf().setAppName("Q2"))
		//val sqlContext = new SQLContext(sc)
		//import sqlContext.implicits._

    	// read the file
    	val file = sc.textFile("hdfs://localhost:8020" + args(0))
		/* TODO: Needs to be implemented */
		  val data = file.map(line => line.split("\t")).filter(_.last.toInt >= 5)
		  val outedge = data.map(line => (line(0), line(2).toInt*(-1))).reduceByKey(_+_,1)
		  val inedge = data.map(line => (line(1), line(2).toInt)).reduceByKey(_+_,1)
		  val edge = outedge.join(inedge)
		  val edgeReduce = edge.mapValues{case (a,b) => a+b}
    	val result = edgeReduce.collect.map(line => line._1 + "\t" + line._2)
    	// store output on given HDFS path.
    	// YOU NEED TO CHANGE THIS
    	sc.makeRDD(result).saveAsTextFile("hdfs://localhost:8020" + args(1))
  	}
}
