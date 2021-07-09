package org.example

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Example7 {

  val logger = Logger(Example1.getClass.getName)
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder.master("local")
      .appName("ExampleApp")
      .getOrCreate()

    // Path to data set
    val txtFile="data/airport-codes-na.txt"
    val jsonFile="data/airport-codes-na.json"
    val airportsna = spark.read.format("com.databricks.spark.csv").
      option("header", "true").
      option("inferschema", "true").
      option("delimiter", "\t").
      load(txtFile)

    val jsonDs = airportsna.toJSON

    val count = jsonDs.count()
    jsonDs
      .repartition(1)
      .rdd
      .zipWithIndex()
      .map { case(json, idx) =>
        if(idx == 0) "[\n" + json + ","
        else if(idx == count-1) json + "\n]"
        else json + ","
      }
      .saveAsTextFile(jsonFile)
    airportsna.show(10)
    airportsna.printSchema()


  }
}
