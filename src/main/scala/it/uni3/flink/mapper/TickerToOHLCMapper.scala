package it.uni3.flink.mapper

import it.uni3.model.{OHLCModel, TickerModel}
import org.apache.flink.api.common.functions.MapFunction

class TickerToOHLCMapper extends MapFunction[TickerModel, OHLCModel] {
  override def map(value: TickerModel): OHLCModel = OHLCModel(
    timestamp = value.time.toEpochMilli,
    high= value.price,
    open = value.price,
    low = value.price,
    close = value.price)
}
