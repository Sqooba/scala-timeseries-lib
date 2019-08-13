package io.sqooba.public.timeseries.archive

import io.sqooba.public.timeseries.TimeSeries
import io.sqooba.public.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

class TSCompressionSpec extends FlatSpec with Matchers {

  val tsDouble = TimeSeries(
    Seq(
      TSEntry(1, 200.03, 100),
      TSEntry(50, 400.03, 100),
      TSEntry(77, 100.03, 100),
      TSEntry(200, 0.123456789, 100)
    )
  )

  val tsLong = TimeSeries(
    Seq(
      TSEntry(1, 1L, 100),
      TSEntry(50, Long.MaxValue, 100),
      TSEntry(77, Long.MinValue, 100),
      TSEntry(200, 123456789L, 100),
      TSEntry(1234123234L, 87767666566L, 100)
    )
  )

  "TSCompression" should "compress and again decompress a timeseries" in {
    val (byteArray1, byteArray2) = TSCompression.compress(tsDouble.entries.toStream)
    TSCompression.decompress(byteArray1, byteArray2) shouldBe tsDouble.entries
  }

  it should "compress and again decompress for converted long values" in {
    val (byteArray1, byteArray2) = TSCompression.compress(tsLong.map(_.toDouble).entries.toStream)

    TSCompression.decompress(byteArray1, byteArray2) shouldBe tsLong.entries
  }

  it should "throw if the byte arrays are not of same length" in {
    val (byteArrayV, byteArrayD) = TSCompression.compress(tsDouble.entries.toStream)

    an[IllegalArgumentException] should be thrownBy
      TSCompression.decompress(
        byteArrayV.drop(8),
        byteArrayD
      )
  }

  it should "throw if an empty stream is provided to compress" in {
    an[IllegalArgumentException] should be thrownBy TSCompression.compress(Stream.empty)
  }

  it should "return an empty stream for empty byte arrays" in {
    an[IllegalArgumentException] should be thrownBy TSCompression.decompress(Array.empty, Array.empty)
  }
}
