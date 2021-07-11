package it.uni3.spark

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

object mongoSpark {

  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/coinbasepro.ticker")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/coinbasepro.ticker")
      .getOrCreate()

    val rdd = MongoSpark.load(spark)

    println("------------------------------")
    println(rdd.show(5))
    println("------------------------------")

  }
}
