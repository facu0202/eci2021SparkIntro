package org.example

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Example4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("ExampleApp")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.textFile("data/LesMiserables.txt",4)
    rdd.take(10).foreach(s=>println(s))
    val formatRdd = rdd.flatMap(line => tokenize(line))
    formatRdd.take(10).foreach(s=>println(s))
    val counts = formatRdd.map(x => (x, 1)).reduceByKey(_ + _)

    counts.sortBy(_._2,ascending=false).take(10).foreach(s=> println(s))

  }

  private def tokenize(text : String) : Array[String] = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
