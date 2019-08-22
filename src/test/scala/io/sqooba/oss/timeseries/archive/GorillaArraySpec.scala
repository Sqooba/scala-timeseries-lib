package io.sqooba.oss.timeseries.archive

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.SortedMap

class GorillaArraySpec extends FlatSpec with Matchers {

  "GorillaArray" should "compress and decompress a timestamp map" in {
    val map = SortedMap(
      10L      -> 234L,
      20L      -> 456L,
      1000000L -> -11L
    )

    GorillaArray.decompressTimestampTuples(
      GorillaArray.compressTimestampTuples(map)
    ) shouldBe map
  }

  it should "throw if an empty map is provided to compress" in {
    an[IllegalArgumentException] should be thrownBy GorillaArray.compressTimestampTuples(SortedMap.empty)
  }

  it should "throw if an empty gorilla array is provided to decompress" in {
    an[IllegalArgumentException] should be thrownBy GorillaArray.decompressTimestampTuples(
      Array.empty
    )
  }
}
