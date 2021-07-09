


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object Example8 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder.master("local")
      .appName("ExampleApp")
      .getOrCreate()

    val jsonFile="data/airport-codes-na.json"
    val csvFile="data/departuredelays.csv"

    val departuredelays = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    val airportcodes = spark.read.format("json")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(jsonFile)

    departuredelays.printSchema()
    departuredelays.show(10)
    airportcodes.printSchema()
    airportcodes.show(10)

    val origin = airportcodes.select(col("City").alias("city_origin"),col("IATA").alias("origin"))

    val joinResult = departuredelays.join(origin, Seq("origin"))

    joinResult.show(10)

  }
}
