package it.uni3.flink.mapper


import com.google.gson._
import it.uni3.model.{OHLCModel, TickerModel}
import org.apache.flink.api.common.functions.MapFunction

import java.lang.reflect.Type
import java.time.Instant

class OHLCModelToJson extends MapFunction[OHLCModel, String] {
  override def map(value: OHLCModel): String = {

    val gson: Gson = new GsonBuilder()
      .registerTypeAdapter(classOf[Instant], new JsonDeserializer[Instant]() {
        @Override
        override def deserialize(json: JsonElement, `type`: Type, jsonDeserializationContext: JsonDeserializationContext): Instant = {
          Instant.parse(json.getAsJsonPrimitive.getAsString)
        }
      }).setPrettyPrinting().create()

    gson.toJson(value)
  }
}