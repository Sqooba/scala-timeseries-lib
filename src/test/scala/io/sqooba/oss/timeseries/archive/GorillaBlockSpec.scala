package io.sqooba.oss.timeseries.archive

import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

class GorillaBlockSpec extends FlatSpec with Matchers {

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

  "GorillaBlock" should "compress and again decompress a timeseries" in {
    val block = GorillaBlock.compress(tsDouble.entries.toStream)
    GorillaBlock.decompress(block) shouldBe tsDouble.entries
  }

  it should "compress and again decompress for converted long values" in {
    val block = GorillaBlock.compress(tsLong.map(_.toDouble).entries.toStream)

    GorillaBlock.decompress(block) shouldBe tsLong.entries
  }

  it should "throw if the byte arrays are not of same length" in {
    val block = GorillaBlock.compress(tsDouble.entries.toStream)

    an[IllegalArgumentException] should be thrownBy
      GorillaBlock.decompress(
        GorillaBlock(block.valueBytes.drop(8), block.validityBytes)
      )
  }

  it should "throw if an empty stream is provided to compress" in {
    an[IllegalStateException] should be thrownBy GorillaBlock.compress(Stream.empty)
  }

  it should "throw if empty byte arrays are provided to decompress" in {
    an[IllegalArgumentException] should be thrownBy GorillaBlock.decompress(
      GorillaBlock(Array.empty, Array.empty)
    )
  }

  it should "throw if the builder is called without any added entries" in {
    an[IllegalStateException] should be thrownBy {
      (new GorillaBlock.Builder()).result()
    }
  }
}
