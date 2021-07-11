package it.uni3.flink.mapper

import it.uni3.model.{OHLCModel, TickerModel}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.Instant

class TickerToOHLCMapperTest extends AnyFlatSpec with should.Matchers {

  "A deser" should "deser ticker json" in {

    val instantTime = Instant.parse("2021-07-02T14:27:14.378461Z")

    val tickerModel = TickerModel(
      "ticker",
      11953085716L,
      "BTC-EUR",
      28274.99,
      28011.13,
      882.84552192,
      27633.18,
      28792.54,
      60836.90210573,
      28274.99,
      28284.47,
      "sell",
      instantTime,
      46375541,
      0.00676199
    )

    val ohlcModel = OHLCModel(
      tickerModel.time.toEpochMilli,
      28274.99,
      28274.99,
      28274.99,
      28274.99
    )

    val mapper = new TickerToOHLCMapper()
    val ohlcModelMapped = mapper.map(tickerModel)

    ohlcModelMapped should be(ohlcModel)
  }
}
