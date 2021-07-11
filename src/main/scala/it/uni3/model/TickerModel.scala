package it.uni3.model

import java.sql.Timestamp
import java.time.Instant

case class TickerModel(
                        `type`: String,
                        sequence: Long,
                        product_id: String,
                        price: Double,
                        open_24h: Double,
                        volume_24h: Double,
                        low_24h: Double,
                        high_24h: Double,
                        volume_30d: Double,
                        best_bid: Double,
                        best_ask: Double,
                        side: String,
                        time: Instant,
                        trade_id: Long,
                        last_size: Double
                 )
