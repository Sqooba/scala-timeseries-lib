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

  val tsSampled = TimeSeries(
    Seq(
      TSEntry(1, 200.03, 100),
      TSEntry(101, 400.03, 100),
      TSEntry(201, 100.03, 100),
      TSEntry(301, 0.123456789, 100)
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

  "GorillaBlock" should "compress and again decompress a timeseries with tuples" in {
    GorillaBlock
      .compress(tsDouble.entries.toStream)
      .decompress shouldBe tsDouble.entries
  }

  it should "compress and again decompress for converted long values with tuples" in {
    GorillaBlock
      .compress(tsLong.map(_.toDouble).entries.toStream)
      .decompress shouldBe tsLong.entries
  }

  "GorillaBlock" should "compress and again decompress a sampled timeseries" in {
    GorillaBlock
      .compressSampled(tsSampled.entries.toStream, 100L)
      .decompress shouldBe tsSampled.entries
  }

  it should "throw if the byte arrays are not of same length" in {
    val TupleGorillaBlock(valueBytes, validityBytes) =
      GorillaBlock.compress(tsDouble.entries.toStream).asInstanceOf[TupleGorillaBlock]

    an[IllegalArgumentException] should be thrownBy (
      GorillaBlock.fromTupleArrays(valueBytes.drop(8), validityBytes).decompress
    )
  }

  it should "throw if an empty stream is provided to compress" in {
    an[IllegalStateException] should be thrownBy GorillaBlock.compress(Stream.empty)
  }

  it should "throw if an empty stream is provided to compress sampled" in {
    an[IllegalStateException] should be thrownBy GorillaBlock.compressSampled(Stream.empty, 10)
  }

  it should "throw if a non-positive sampling rate is given to compress" in {
    an[IllegalArgumentException] should be thrownBy
      GorillaBlock.compressSampled(tsSampled.entries.toStream, 0)

    an[IllegalArgumentException] should be thrownBy
      GorillaBlock.compressSampled(tsSampled.entries.toStream, -100)
  }

  it should "throw if empty byte arrays are provided to decompress" in {
    an[IllegalArgumentException] should be thrownBy (
      GorillaBlock.fromTupleArrays(Array.empty[Byte], Array.empty[Byte]).decompress
    )
  }

  it should "throw if empty byte arrays are provided to decompress sampled" in {
    an[IllegalArgumentException] should be thrownBy (
      GorillaBlock.fromSampled(Array.empty[Byte], 100).decompress
    )
  }

  it should "throw if a non-positive sampling rate is given to the constructor" in {
    an[IllegalArgumentException] should be thrownBy GorillaBlock.fromSampled(Array(1, 2, 3), -100)
  }

  it should "throw if the builder is called without any added entries" in {
    an[IllegalStateException] should be thrownBy {
      GorillaBlock.newBuilder.result()
    }
  }
}
