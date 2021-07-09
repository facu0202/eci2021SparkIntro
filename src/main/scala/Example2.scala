package org.example

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Example2 {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local")
      .appName("ExampleApp")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.textFile("data/LesMiserables.txt",4)
     rdd.take(100).foreach(s=>println("Linea "+s))
     val l =  rdd.glom().collect()
    println(l.toString)
  }
}
