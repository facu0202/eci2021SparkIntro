

import org.apache.spark.sql.SparkSession


object Example3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder.master("local")
      .appName("ExampleApp")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(1 to 100)

    val rdd2 = rdd.filter(v => v % 2 == 0)

    val rdd3 = rdd.map(v => v * 2 )

    val rdd4 = rdd.groupBy(v => v % 2 == 0 )

    rdd4.foreach(v =>println( v._1 +"-> "+v._2.toList))

    val l =  rdd2.glom().collect()
    println(l.toString)


  }
}
