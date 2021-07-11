package it.uni3.flink

import it.uni3.common.Utils.coinbase_ticker_group

import java.util.Properties

object FlinkKafkaProperties {
  var properties: Option[Properties] = None

  def getProperties: Properties ={
    properties.getOrElse(createProperties)
  }

  private def createProperties: Properties = {
    properties = Some(new Properties())
    properties.get.setProperty("bootstrap.servers", "localhost:9092")
    properties.get.setProperty("group.id", coinbase_ticker_group)
    properties.get
  }
}
