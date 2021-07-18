package it.uni3.spark

import com.google.gson.{Gson, GsonBuilder, JsonDeserializationContext, JsonDeserializer, JsonElement}
import com.mongodb.spark.MongoSpark
import it.uni3.flink.mapper.TickerJsonToTickerModelMapper
import it.uni3.model.TickerModel
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.sparkDatasetFunctions
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.lang.reflect.Type
import java.time.Instant

import org.apache.spark.sql.functions.{lit, schema_of_json, from_json}
import collection.JavaConverters._

object mongo2elastic {

  // square of an integer
  def valueFromJson(value:String):TickerModel
  =
  {
    val gson: Gson = new GsonBuilder()
      .registerTypeAdapter(classOf[Instant], new JsonDeserializer[Instant]() {
        @Override
        override def deserialize(json: JsonElement, `type`: Type, jsonDeserializationContext: JsonDeserializationContext): Instant = {
          Instant.parse(json.getAsJsonPrimitive.getAsString)
        }
      }).setPrettyPrinting().create()

    gson.fromJson(value, classOf[TickerModel])

  }

  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .master("local")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/coinbasepro.ticker")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/coinbasepro.ticker")
      .config("spark.es.nodes","127.0.0.1")
      .config("spark.es.port","9200")
      .config("spark.es.nodes.wan.only","true") //Needed for ES on AWS
      .getOrCreate()

    import spark.implicits._

    val mongoDf = MongoSpark.load(spark)

    val schema = schema_of_json(lit(mongoDf.select("value").as[String].first))
    val mappedDF = mongoDf.withColumn("value", from_json($"value", schema, Map[String, String]().asJava))

    val dfToSchema = mappedDF
      .withColumn("type", $"value.type")
      .withColumn("sequence", $"value.sequence")
      .withColumn("product_id", $"value.product_id")
      .withColumn("price", $"value.price".cast("Double"))
      .withColumn("open_24h", $"value.open_24h".cast("Double"))
      .withColumn("volume_24h", $"value.volume_24h".cast("Double"))
      .withColumn("low_24h", $"value.low_24h".cast("Double"))
      .withColumn("high_24h", $"value.high_24h".cast("Double"))
      .withColumn("volume_30d", $"value.volume_30d".cast("Double"))
      .withColumn("best_bid", $"value.best_bid".cast("Double"))
      .withColumn("best_ask", $"value.best_ask".cast("Double"))
      .withColumn("side", $"value.side")
      .withColumn("time", $"value.time".cast("Timestamp").cast(DataTypes.LongType))
      .withColumn("trade_id", $"value.trade_id".cast("Long"))
      .withColumn("last_size", $"value.last_size".cast("Double"))
      .drop("value")


    dfToSchema.createOrReplaceTempView("bitcoinView")

    // prezzo minimo e prezzo massimo
    val sqlMinMaxPrice = spark.sql("SELECT MIN(price) as PrezzoMinimo, MAX(price) as PrezzoMassimo FROM bitcoinView")
    sqlMinMaxPrice.show()

    // data prima quotazione
    val dataPrimaQuotazioneDF = spark.sql("SELECT product_id, MIN(time) as dataPrimaQuotazione FROM bitcoinView GROUP BY product_id")
    //dataPrimaQuotazioneDF.show()
    dataPrimaQuotazioneDF.createOrReplaceTempView("prima_quotazione")

    // data ultima quotazione
    val dataUltimaQuotazioneDF = spark.sql("SELECT product_id, MAX(time) as dataUltimaQuotazione FROM bitcoinView GROUP BY product_id")
    dataUltimaQuotazioneDF.show()
    dataUltimaQuotazioneDF.createOrReplaceTempView("ultima_quotazione")

    // tabella prezzo - data prima quotazione
    val prezziChiusuraIniziali = spark.sql("SELECT bitcoinView.product_id, price, dataPrimaQuotazione" +
      " FROM bitcoinView, prima_quotazione WHERE time = dataPrimaQuotazione LIMIT 1")
    prezziChiusuraIniziali.createOrReplaceTempView("prezziChiusuraIniziali")

    // tabella prezzo - data ultima quotazione
    val prezziChiusuraFinali = spark.sql("SELECT bitcoinView.product_id, price, dataUltimaQuotazione" +
      " FROM bitcoinView, ultima_quotazione WHERE time = dataUltimaQuotazione LIMIT 1")
    prezziChiusuraFinali.createOrReplaceTempView("prezziChiusuraFinali")

    // calcolo variazione percentuale
    val variazionePercentuale = spark.sql("SELECT prezziChiusuraFinali.product_id, " +
      "((prezziChiusuraFinali.price - prezziChiusuraIniziali.price)/prezziChiusuraIniziali.price)*100 as " +
      "variazione_percentuale, dataPrimaQuotazione, dataUltimaQuotazione FROM prezziChiusuraFinali, " +
      "prezziChiusuraIniziali WHERE prezziChiusuraFinali.product_id = prezziChiusurainiziali.product_id")


    // distribuzione cumulativa
    val sqlDistribuzionePrezzi = spark.sql("SELECT product_id, sequence, price, time, " +
      "CUME_DIST () OVER (PARTITION BY product_id ORDER BY price) AS CumeDist FROM bitcoinView " +
      "ORDER BY time")


    // elasticSearch: variazione percentuale
    variazionePercentuale.saveToEs("variazionepercentuale")

    // elasticSearch: distribuzione cumulativa
    sqlDistribuzionePrezzi.saveToEs("distribuzionecumulativa")

    // elasticSearch: prezzo minimo e massimo storico
    sqlMinMaxPrice.saveToEs("minmaxprice")

//    variazionePercentuale.show(false)
//    sqlDistribuzionePrezzi.show(false)

  }
}
