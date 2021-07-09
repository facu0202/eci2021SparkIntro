package org.example


import org.apache.spark.sql.SparkSession

object Example1 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder.master("local")
      .appName("ExampleApp")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(1 to 100,4)
    println(s"Number of partitions: ${rdd.getNumPartitions}")
     val l =  rdd.glom().collect()
    println(l.toString)
  }
}
