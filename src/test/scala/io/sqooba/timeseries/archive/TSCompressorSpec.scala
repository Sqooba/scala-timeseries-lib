package io.sqooba.public.timeseries.archive

<<<<<<< HEAD:common/timeseries/src/test/scala/io/sqooba/public/timeseries/archive/TSCompressionSpec.scala
import io.sqooba.public.timeseries.TimeSeries
import io.sqooba.public.timeseries.immutable.TSEntry
=======
import io.sqooba.timeseries.TimeSeries
import io.sqooba.timeseries.archive.TSCompressor.GorillaBlock
import io.sqooba.timeseries.immutable.TSEntry
>>>>>>> [timeseries] Add tooling for splitting series by intervals etc.:common/timeseries/src/test/scala/io/sqooba/timeseries/archive/TSCompressorSpec.scala
import org.scalatest.{FlatSpec, Matchers}

class TSCompressorSpec extends FlatSpec with Matchers {

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
    val block = TSCompressor.compress(tsDouble.entries.toStream)
    TSCompressor.decompress(block) shouldBe tsDouble.entries
  }

  it should "compress and again decompress for converted long values" in {
    val block = TSCompressor.compress(tsLong.map(_.toDouble).entries.toStream)

    TSCompressor.decompress(block) shouldBe tsLong.entries
  }

  it should "throw if the byte arrays are not of same length" in {
    val block = TSCompressor.compress(tsDouble.entries.toStream)

    an[IllegalArgumentException] should be thrownBy
      TSCompressor.decompress(
        GorillaBlock(block.valueBytes.drop(8), block.validityBytes)
      )
  }

  it should "throw if an empty stream is provided to compress" in {
    an[IllegalArgumentException] should be thrownBy TSCompressor.compress(Stream.empty)
  }

  it should "return an empty stream for empty byte arrays" in {
    an[IllegalArgumentException] should be thrownBy TSCompressor.decompress(
      GorillaBlock(Array.empty, Array.empty)
    )
  }
}
