package it.uni3.flink.mapper

import it.uni3.model.{OHLCModel, TickerModel}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}

class TickerJsonToTickerModelMapperTest extends AnyFlatSpec with should.Matchers {

  "A deser" should "deser ticker json" in {

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
      Instant.parse("2021-07-02T14:27:14.378461Z"),
      46375541,
      0.00676199
    )

    val json = "{\"type\":\"ticker\",\"sequence\":11953085716,\"product_id\":\"BTC-EUR\",\"price\":\"28274.99\",\"open_24h\":\"28011.13\",\"volume_24h\":\"882.84552192\",\"low_24h\":\"27633.18\",\"high_24h\":\"28792.54\",\"volume_30d\":\"60836.90210573\",\"best_bid\":\"28274.99\",\"best_ask\":\"28284.47\",\"side\":\"sell\",\"time\":\"2021-07-02T14:27:14.378461Z\",\"trade_id\":46375541,\"last_size\":\"0.00676199\"}"

    val mapper = new TickerJsonToTickerModelMapper()
    val tickerMapped = mapper.map(json)

    tickerMapped should be(tickerModel)
  }
}
