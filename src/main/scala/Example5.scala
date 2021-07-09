package org.example

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Example5 {

  val logger = Logger(Example1.getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder.master("local")
      .appName("ExampleApp")
      .getOrCreate()
    val sc = spark.sparkContext

    // Joiner!
    val data = sc.parallelize(Array(("A",1),("b",2),("c",3)))
    val data2 =sc.parallelize(Array(("A",4),("A",6),("b",7),("c",3),("c",8)))
    val result = data.join(data2)
    println(result.collect().mkString(","))


  }
}
