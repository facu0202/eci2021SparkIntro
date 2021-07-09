package org.example

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Example6 {

  val logger = Logger(Example1.getClass.getName)
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder.master("local")
      .appName("SparkSQLExampleApp")
      .getOrCreate()

    val csvFile="data/departuredelays.csv"

    val departuredelays = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    departuredelays.printSchema()

    departuredelays.createOrReplaceTempView("us_delay_flights_tbl")

    departuredelays.select("distance").show(10)

    spark.sql("""SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)

  }
}
