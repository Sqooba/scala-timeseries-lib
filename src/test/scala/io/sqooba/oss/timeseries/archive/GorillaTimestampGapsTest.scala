package io.sqooba.oss.timeseries.archive

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.SortedMap

class GorillaTimestampGapsTest extends FlatSpec with Matchers {

  private val oneDayinMilliseconds: Long = 24 * 3600 * 1000

  "Gorilla compression" should "be able to have a gap of one day in ms" in {
    val map = SortedMap(
      0L                   -> 12L,
      oneDayinMilliseconds -> 23L
    )

    GorillaArray.decompressTimestampTuples(
      GorillaArray.compressTimestampTuples(map)
    ) shouldBe map
  }

  it should "be able to have a gap of 27bits in total" in {
    val maximalGap: Long = Integer.parseInt("1" * 27, 2).toLong
    maximalGap shouldBe 134217727

    val mapThatWorks = SortedMap(
      0L             -> 12L,
      maximalGap - 1 -> 23L
    )

    GorillaArray.decompressTimestampTuples(
      GorillaArray.compressTimestampTuples(mapThatWorks)
    ) shouldBe mapThatWorks

    val mapThatFails = SortedMap(
      0L         -> 12L,
      maximalGap -> 23L
    )

    GorillaArray.decompressTimestampTuples(
      GorillaArray.compressTimestampTuples(mapThatFails)
    ) shouldBe mapThatFails.take(1)
  }

  it should "be able to have a gap of 32bits in total if not starting at 0" in {
    val maxValue = Int.MaxValue.toLong
    val startTs  = 10000L

    val mapThatWorks = SortedMap(
      startTs            -> 12L,
      maxValue + startTs -> 23L
    )

    GorillaArray.decompressTimestampTuples(
      GorillaArray.compressTimestampTuples(mapThatWorks)
    ) shouldBe mapThatWorks

    val mapThatFails = SortedMap(
      startTs                -> 12L,
      maxValue + startTs + 1 -> 23L
    )

    GorillaArray.decompressTimestampTuples(
      GorillaArray.compressTimestampTuples(mapThatFails)
    ) should not be mapThatFails
  }

  it should "wrongly parse a 0 timestamp that is not at the start" in {
    val map = SortedMap(
      -oneDayinMilliseconds + 10000 -> -11L,
      0L                            -> 12L,
      oneDayinMilliseconds          -> 23L
    )

    GorillaArray.decompressTimestampTuples(
      GorillaArray.compressTimestampTuples(map)
    ) should not be map
  }
}
