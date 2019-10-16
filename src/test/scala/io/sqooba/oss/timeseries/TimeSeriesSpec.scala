package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.scalatest.{FlatSpec, Matchers}

/** This tests all the methods that are implemented on the TimeSeries trait itself or
  * on its companion object.
  */
class TimeSeriesSpec extends FlatSpec with Matchers {

  // Simple non-strict summing operator
  private def plus(aO: Option[Double], bO: Option[Double]): Option[Double] = {
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a + b)
      case (Some(_), None)    => aO
      case (None, Some(_))    => bO
      case _                  => None
    }
  }

  private def mul(aO: Option[Double], bO: Option[Double]): Option[Double] = {
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a.doubleValue * b.doubleValue)
      case (Some(x), None)    => Some(x)
      case (None, Some(x))    => Some(x)
      case _                  => None
    }
  }

  "TimeSeries" should "correctly do Slice" in {
    val tri =
      VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0, "Hi", 10), TSEntry(10, "Ho", 10L), TSEntry(20, "Hu", 10L)))
    assert(tri.slice(-1, 0) == EmptyTimeSeries)
    assert(tri.slice(-1, 10).entries == Seq(TSEntry(0, "Hi", 10)))
    assert(tri.slice(0, 10).entries == Seq(TSEntry(0, "Hi", 10)))
    assert(tri.slice(0, 9).entries == Seq(TSEntry(0, "Hi", 9)))
    assert(tri.slice(1, 10).entries == Seq(TSEntry(1, "Hi", 9)))
    assert(tri.slice(9, 11).entries == Seq(TSEntry(9, "Hi", 1), TSEntry(10, "Ho", 1)))
    assert(tri.slice(9, 20).entries == Seq(TSEntry(9, "Hi", 1), TSEntry(10, "Ho", 10)))
    assert(tri.slice(10, 20).entries == Seq(TSEntry(10, "Ho", 10)))
    assert(tri.slice(15, 20).entries == Seq(TSEntry(15, "Ho", 5)))
    assert(tri.slice(15, 25).entries == Seq(TSEntry(15, "Ho", 5), TSEntry(20, "Hu", 5)))
    assert(tri.slice(20, 25).entries == Seq(TSEntry(20, "Hu", 5)))
    assert(tri.slice(25, 30).entries == Seq(TSEntry(25, "Hu", 5)))
    assert(tri.slice(25, 35).entries == Seq(TSEntry(25, "Hu", 5)))

  }

  it should "correctly do SliceDiscrete" in {
    val tri =
      VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0, "Hi", 10), TSEntry(10, "Ho", 10L), TSEntry(20, "Hu", 10L)))

    assert(tri == tri.sliceDiscrete(5, 25, true, true))
    assert(tri.slice(0, 20) == tri.sliceDiscrete(5, 25, true, false))
    assert(tri.slice(10, 30) == tri.sliceDiscrete(5, 25, false, true))
    assert(tri.slice(10, 20) == tri.sliceDiscrete(5, 25, false, false))

    assert(TSEntry(10, "Ho", 10) == tri.sliceDiscrete(10, 20, true, true))
    assert(TSEntry(10, "Ho", 10) == tri.sliceDiscrete(10, 20, false, false))
  }

  it should "correctly do SplitDiscrete" in {
    val tri =
      VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0, "Hi", 10), TSEntry(10, "Ho", 10L), TSEntry(20, "Hu", 10L)))

    assert(
      (VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0L, "Hi", 10L), TSEntry(10, "Ho", 10L))), TSEntry(20, "Hu", 10L))
        === tri.splitDiscrete(15, true)
    )

    assert(
      (TSEntry(0L, "Hi", 10L), VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(10, "Ho", 10L), TSEntry(20, "Hu", 10L))))
        == tri.splitDiscrete(15, false)
    )

    assert(
      (tri, EmptyTimeSeries)
        == tri.splitDiscrete(25, true)
    )

    assert(
      (VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0L, "Hi", 10L), TSEntry(10, "Ho", 10L))), TSEntry(20, "Hu", 10L))
        == tri.splitDiscrete(25, false)
    )

    assert(
      (TSEntry(0L, "Hi", 10L), VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(10, "Ho", 10L), TSEntry(20, "Hu", 10L))))
        == tri.splitDiscrete(5, true)
    )

    assert(
      (EmptyTimeSeries, tri) == tri.splitDiscrete(5, false)
    )

  }

  it should "correctly do FillWithoutCompression" in {
    // Simple cases: 0 and 1 entries
    assert(TimeSeries.fillGaps(Seq(), 0) == Seq())
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10)), 0) ==
        Seq(TSEntry(1, 1, 10))
    )

    // Two values, contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10))
    )

    // Three values, contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10), TSEntry(21, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10), TSEntry(21, 3, 10))
    )

    // Two values, non-contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 0, 10), TSEntry(21, 3, 10))
    )

    // Three values, non-contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(41, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 0, 10), TSEntry(21, 2, 10), TSEntry(31, 0, 10), TSEntry(41, 3, 10))
    )

    // Three values, two first non-contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(31, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 0, 10), TSEntry(21, 2, 10), TSEntry(31, 3, 10))
    )

  }

  it should "correctly do FillWithCompression" in {
    // Simple cases: 0 and 1 entries
    assert(TimeSeries.fillGaps(Seq(), 0) == Seq())
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10)), 0) ==
        Seq(TSEntry(1, 1, 10))
    )

    // Two values, non-contiguous, fill value extends previous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 3, 10)), 1) ==
        Seq(TSEntry(1, 1, 20), TSEntry(21, 3, 10))
    )

    // Two values, non-contiguous, fill value advances next
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 3, 10)), 3) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 3, 20))
    )

    // Two values, non-contiguous, fill value bridges both values
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 1, 10)), 1) ==
        Seq(TSEntry(1, 1, 30))
    )

    // Three values, non-contiguous, extend first and advance last
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(41, 1, 10)), 1) ==
        Seq(TSEntry(1, 1, 20), TSEntry(21, 2, 10), TSEntry(31, 1, 20))
    )

    // Three values, non-contiguous, both advance and extend middle one
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(41, 1, 10)), 2) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 30), TSEntry(41, 1, 10))
    )

    // Three values, non-contiguous, bridge every one
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 1, 10), TSEntry(41, 1, 10)), 1) ==
        Seq(TSEntry(1, 1, 50))
    )
  }

  it should "correctly do ApplyWithUnsortedEntries" in {
    val entries = List(
      TSEntry(5, 5, 1),
      TSEntry(1, 1, 1)
    )

    val resultSeries = TimeSeries(entries)

    assert(resultSeries.entries.head == entries.last)
    assert(resultSeries.entries.last == entries.head)
  }

  it should "correctly do ApplyShouldFailWithTwoEntriesHavingSameTimestamps" in {
    assertThrows[IllegalArgumentException](TimeSeries(Seq(TSEntry(1, 1, 1), TSEntry(1, 1, 1))))
  }

  it should "correctly do AppendPrependWithEmptyShouldBeTheSame" in {
    val ts = TimeSeries(
      Seq(
        TSEntry(1, 1, 1),
        TSEntry(2, 2, 1)
      )
    )

    assert(ts.append(EmptyTimeSeries) == ts)
    assert(ts.prepend(EmptyTimeSeries) == ts)
  }

  it should "correctly do AppendPrependWithOutOfDomainShouldReturnArgument" in {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 1, 1),
        TSEntry(2, 2, 1)
      )
    )

    val ts2 = TimeSeries(
      Seq(
        TSEntry(3, 3, 1),
        TSEntry(4, 4, 1)
      )
    )

    assert(ts2.append(ts1) == ts1)
    assert(ts1.prepend(ts2) == ts2)
  }

  it should "correctly do AppendPrependShouldTrimIfNeeded" in {
    val ts1 = TSEntry(0, 1, 10)
    val ts2 = TSEntry(5, 2, 10)

    assert(ts1.append(ts2).entries.head.validity == 5)
    assert(ts2.prepend(ts1).entries.last.timestamp == 10)
  }

  it should "correctly do AppendPrependShouldCompress" in {
    val ts1 = TSEntry(1, 1, 1)
    val ts2 = TSEntry(2, 1, 1)

    val result = TSEntry(1, 1, 2)

    assert(ts1.append(ts2) == result)
    assert(ts2.prepend(ts1) == result)
  }

  it should "correctly do FallbackUncompress" in {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 'a', 1),
        TSEntry(3, 'c', 1)
      )
    )

    val ts2 = TSEntry(1, 'b', 3)

    assert(
      ts1.fallback(ts2).entries == Seq(
        TSEntry(1, 'a', 1),
        TSEntry(2, 'b', 1),
        TSEntry(3, 'c', 1)
      )
    )
  }

  it should "correctly do FallbackCompress" in {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 'a', 1),
        TSEntry(3, 'a', 1)
      )
    )

    val ts2 = TSEntry(1, 'a', 3)

    assert(ts1.fallback(ts2) == ts2)
  }

  it should "correctly do DifferentSupportRatio" in {
    assert(EmptyTimeSeries.supportRatio == 0)
    assert(TSEntry(1, 'a', 123098).supportRatio == 1)

    val ts = TimeSeries(
      Seq(
        TSEntry(0, 'a', 2),
        TSEntry(3, 'a', 1)
      )
    )

    val ts2 = TSEntry(1, 'a', 3)

    assert(ts.supportRatio == 0.75)
  }

  it should "correctly do StrictMerge" in {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, "hel", 5),
        TSEntry(10, "hel", 5)
      ))

    val ts2 = TimeSeries(
      Seq(
        TSEntry(1, "lo", 9),
        TSEntry(12, "lo", 8)
      ))

    assert(
      ts1.strictMerge[String, String](_ + _)(ts2).entries == Seq(TSEntry(1, "hello", 5), TSEntry(12, "hello", 3))
    )
  }

  it should "correctly do StrictMergeDisjoint" in {
    val ts1 = TimeSeries(Seq(TSEntry(1, "hel", 5)))
    val ts2 = TimeSeries(Seq(TSEntry(6, "lo", 5)))

    assert(
      ts1.strictMerge[String, String](_ + _)(ts2).entries == Seq()
    )
  }

  it should "correctly do MathOperationsCompressedResult" in {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 0, 1),
        TSEntry(2, 1, 1)
      )
    )

    val ts2 = TimeSeries(
      Seq(
        TSEntry(1, 1, 1),
        TSEntry(2, 0, 1)
      )
    )

    assert(ts1.plus(ts2) == TSEntry(1, 1, 2))
    assert(ts1.minus(ts1) == TSEntry(1, 0, 2))
    assert(ts1.multiply(ts2) == TSEntry(1, 0, 2))
  }

  it should "correctly do MinusWithDefaults" in {
    val tsLeft  = TimeSeries(Seq(TSEntry(1, 1, 1), TSEntry(3, 1, 1)))
    val tsRight = TimeSeries(Seq(TSEntry(2, 5, 2)))

    assert(
      tsLeft.minus(tsRight, leftHandDefault = Some(2)).entries ==
        Seq(TSEntry(2, -3, 1), TSEntry(3, -4, 1))
    )
  }

  it should "correctly do BucketByNumber" in {
    val entries = Stream(
      TSEntry(0L, 10, 80L),
      TSEntry(100L, 22, 20L),
      TSEntry(120L, 3, 40L),
      TSEntry(160L, -7, 20L),
      TSEntry(180L, -3, 20L)
    )

    assert(
      TimeSeries.groupEntries(Stream.empty, 2) === Stream.empty
    )

    assert(
      TimeSeries.groupEntries(entries, 10) ===
        Stream((0L, entries))
    )

    assert(
      TimeSeries.groupEntries(entries, 2) ===
        Stream(
          (0L, entries.slice(0, 2)),
          (120L, entries.slice(2, 4)),
          (180L, entries.slice(4, 5))
        )
    )
  }

  "TimeSeries.sample" should "output an empty series for an empty input" in {
    EmptyTimeSeries.sample(1000, 10, useClosestInWindow = false) shouldBe EmptyTimeSeries
    EmptyTimeSeries.sample(1000, 10, useClosestInWindow = true) shouldBe EmptyTimeSeries
  }

  "TimeSeries.sample without using closest" should "take the values at the sample points" in {
    val sampleRate = 100
    val series = TimeSeries(
      Seq(
        TSEntry(100, .123, 90),
        TSEntry(190, .234, 50),
        TSEntry(200, .345, 10),
        TSEntry(250, .456, 100)
      )
    )

    series.sample(0, sampleRate, useClosestInWindow = false).entries shouldBe Seq(
      TSEntry(100, .123, sampleRate),
      TSEntry(200, .345, sampleRate),
      TSEntry(300, .456, sampleRate)
    )

    series.sample(20, sampleRate, useClosestInWindow = false).entries shouldBe Seq(
      TSEntry(120, .123, sampleRate),
      TSEntry(320, .456, sampleRate)
    )

    val shortSampleRate = 35
    series.sample(20, shortSampleRate, useClosestInWindow = false).entries shouldBe Seq(
      TSEntry(125, 0.123, shortSampleRate),
      TSEntry(160, 0.123, shortSampleRate),
      TSEntry(195, 0.234, shortSampleRate),
      TSEntry(265, 0.456, shortSampleRate),
      TSEntry(300, 0.456, shortSampleRate),
      TSEntry(335, 0.456, shortSampleRate)
    )
  }

  it should "correctly handle edges of the entry domain" in {
    val series = TSEntry(1, .123, 9)

    series.sample(1, 10, useClosestInWindow = false).entries shouldBe Seq(TSEntry(1, .123, 10))
    series.sample(1, 9, useClosestInWindow = false).entries shouldBe Seq(TSEntry(1, .123, 9))
    series.sample(1, 8, useClosestInWindow = false).entries shouldBe Seq(TSEntry(1, 0.123, 8), TSEntry(9, 0.123, 8))
  }

  "TimeSeries.sample with using closest" should "split long entries" in {
    TSEntry(10, .789, 100).sample(5, 25, useClosestInWindow = true).entries shouldBe Seq(
      TSEntry(5, .789, 25),
      TSEntry(30, .789, 25),
      TSEntry(55, .789, 25),
      TSEntry(80, .789, 25),
      TSEntry(105, .789, 25)
    )
  }

  it should "take the closest if undefined at the sample point" in {
    val series = TimeSeries(Seq(TSEntry(0, .123, 5), TSEntry(10, .234, 6)))

    series.sample(7, 8, useClosestInWindow = true).entries shouldBe Seq(
      TSEntry(7, .234, 8),
      TSEntry(15, .234, 8)
    )
  }

  it should "take the closest if the previous entry is defined, but the next one starts within a half sample period" in {
    val series = TimeSeries(Seq(TSEntry(4, .123, 7), TSEntry(14, .234, 8), TSEntry(23, .345, 8)))

    series.sample(0, 10, useClosestInWindow = true).entries shouldBe Seq(
      TSEntry(0, .123, 10),
      TSEntry(10, .234, 10),
      TSEntry(20, .345, 10),
      TSEntry(30, .345, 10)
    )
  }

  it should "take the value of the entry that starts closest to the sample point among those entries starting in the " +
    "window of half a sample period from the sample point" in {
    val series = TimeSeries(Seq(TSEntry(7, .123, 2), TSEntry(14, .234, 6)))

    series.sample(0, 10, useClosestInWindow = true).entries shouldBe Seq(
      TSEntry(10, .123, 10)
    )

    val series2 = TimeSeries(Seq(TSEntry(6, .123, 3), TSEntry(13, .234, 6)))

    series2.sample(0, 10, useClosestInWindow = true).entries shouldBe Seq(
      TSEntry(10, .234, 10)
    )

    val series3 = TimeSeries(Seq(TSEntry(6, .123, 1), TSEntry(7, .234, 1), TSEntry(11, .345, 1)))

    series3.sample(0, 10, useClosestInWindow = true).entries shouldBe Seq(
      TSEntry(10, .345, 10)
    )
  }

  it should "correctly drop some short entries" in {
    val series = TimeSeries(
      Seq(
        TSEntry(1, .012, 2),
        TSEntry(5, .123, 1),
        TSEntry(6, .234, 3),
        TSEntry(10, .345, 2),
        TSEntry(14, .456, 4),
        TSEntry(21, .567, 4)
      )
    )

    val sampleRate = 10
    series.sample(0, sampleRate, useClosestInWindow = true).entries shouldBe Seq(
      TSEntry(0, .012, sampleRate),
      TSEntry(10, .345, sampleRate),
      TSEntry(20, .567, sampleRate)
    )
  }

  it should "compress equal contiguous entries if flag set" in {
    val series = TimeSeries(Seq(TSEntry(4, .123, 7), TSEntry(14, .234, 8), TSEntry(23, .345, 8)))

    series.sample(0, 10, useClosestInWindow = true, compress = true).entries shouldBe Seq(
      TSEntry(0, .123, 10),
      TSEntry(10, .234, 10),
      TSEntry(20, .345, 20)
    )
  }
}
