package it.uni3.flink.mapper

import it.uni3.model.TickerModel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}

class DateTimeParserTest extends AnyFlatSpec with should.Matchers {

  "A deser" should "deser ticker json" in {

    val stringTimestamp = "2021-07-02T14:27:14.378461Z"
//    val format = "yyyy-MM-ddTHH:mm:ss.SSSSSSZ"
//
//    val formatter = DateTimeFormatter.ofPattern(format)
//
//    val dateTime = LocalDateTime.parse(stringTimestamp, formatter)
    print(Instant.now.toString)
    val a = Instant.parse(stringTimestamp)
    print(a)
  }
}
