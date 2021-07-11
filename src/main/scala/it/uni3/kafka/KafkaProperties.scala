package it.uni3.kafka

import java.util.Properties

object KafkaProperties {
  var properties: Option[Properties] = None

  def getProperties: Properties ={
    properties.getOrElse(createProperties)
  }

  private def createProperties: Properties = {
    properties = Some(new Properties())
    properties.get.put("bootstrap.servers", "localhost:9092")
    properties.get.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.get.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.get
  }
}
