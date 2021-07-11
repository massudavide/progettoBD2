package it.uni3.flink.mapper

import it.uni3.model.OHLCModel
import org.apache.flink.api.common.functions.ReduceFunction

class OHLCReduceFunction extends ReduceFunction[OHLCModel] {
  override def reduce(t: OHLCModel, t1: OHLCModel): OHLCModel = {

    var open_price: Double = 0
    if (t.timestamp <= t1.timestamp)
      open_price = t.open
    else open_price = t1.open

    var close_price: Double = 0
    if (t.timestamp >= t1.timestamp)
      close_price = t.close
    else close_price = t1.close

    var high_price: Double = 0
    if (t.high >= t1.high)
      high_price = t.high
    else high_price = t1.high

    var low_price: Double = 0
    if (t.low <= t1.low)
      low_price = t.low
    else low_price = t1.close

    OHLCModel(
      timestamp = t1.timestamp,
      open = open_price,
      high = high_price,
      low = low_price,
      close = close_price)
  }
}
