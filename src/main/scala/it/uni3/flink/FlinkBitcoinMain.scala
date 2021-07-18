package it.uni3.flink

import com.google.gson.Gson
import it.uni3.common.Utils
import it.uni3.flink.mapper.{OHLCModelToJson, OHLCReduceFunction, TickerJsonToTickerModelMapper, TickerToOHLCMapper}
import it.uni3.model.OHLCModel
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.time.Duration


object FlinkBitcoinMain {

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = FlinkKafkaProperties.getProperties

    val kafkaConsumer = new FlinkKafkaConsumer[String](Utils.coinbase_ticker_topic, new SimpleStringSchema(), properties)
      .setStartFromEarliest()

    val kafkaStream = env.addSource(kafkaConsumer)

    val kafka2ohlc = kafkaStream.map(new TickerJsonToTickerModelMapper).map(new TickerToOHLCMapper)

    val strategy = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(20))
      .withTimestampAssigner(new SerializableTimestampAssigner[OHLCModel] {
        override def extractTimestamp(element: OHLCModel, recordTimestamp: Long): Long = element.timestamp
      })

    val timedTicker = kafka2ohlc.assignTimestampsAndWatermarks(strategy)
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(new OHLCReduceFunction())

    val tickerToJson = timedTicker.map(new OHLCModelToJson)

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
          val gson = new Gson
          val json = gson.fromJson(element, (new java.util.HashMap[String, Any]()).getClass)
          println(json)
          val rqst: IndexRequest = Requests.indexRequest
            .index("bitcoin-ohlc")
            .source(json)

          indexer.add(rqst)
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    tickerToJson.addSink(esSinkBuilder.build)

    env.execute
  }

}
