package io.sqooba.oss.timeseries.archive

import io.sqooba.oss.timeseries.bucketing.TimeBucketer
import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

class TimeBucketerSpec extends FlatSpec with Matchers {

  private val entries = Stream(
    TSEntry(0L, 10, 80L),
    TSEntry(100L, 22, 20L),
    TSEntry(120L, 3, 40L),
    TSEntry(160L, -7, 20L),
    TSEntry(180L, -3, 20L)
  )

  "TSBuckets" should "bucket entries by timestamp and by number" in {
    TimeBucketer.bucketEntries(
      entries,
      Stream.from(0, 100).map(_.toLong),
      maxNumberOfEntries = 2
    ) shouldBe
      Stream(
        TSEntry(0L, entries.slice(0, 1), 100),
        TSEntry(100L, entries.slice(1, 3), 60),
        TSEntry(160L, entries.slice(3, 5), 40)
      )
  }

  it should "throw for an empty stream of entries" in {
    an[IllegalArgumentException] should be thrownBy
      TimeBucketer.bucketEntries(
        Stream.empty,
        Stream.from(0, 100).map(_.toLong),
        1
      )
  }

  it should "bucket only by time if the number limit is not reached" in {
    TimeBucketer.bucketEntries(
      entries,
      Stream.from(0, 100).map(_.toLong),
      maxNumberOfEntries = 100
    ) shouldBe
      Stream(
        TSEntry(0L, entries.slice(0, 1), 100),
        TSEntry(100L, entries.slice(1, 5), 100)
      )
  }

  it should "bucket only by number if the time limit is not reached" in {
    TimeBucketer.bucketEntries(
      entries,
      Stream.from(0, 200).map(_.toLong),
      maxNumberOfEntries = 2
    ) shouldBe
      Stream(
        TSEntry(0L, entries.slice(0, 2), 120),
        TSEntry(120L, entries.slice(2, 4), 60),
        TSEntry(180L, entries.slice(4, 5), 20)
      )
  }

  private val str = Stream.from(1, 10).map(_.toLong)

  it should "correctly terminate the bucket stream" in {
    assert(
      Seq((1L, Seq())) ===
        TimeBucketer.bucketEntries(str, Seq())
    )
  }

  it should "throw if an entry has a timestamp that is in the past" in {
    assertThrows[IllegalArgumentException](
      TimeBucketer.bucketEntries(str, Seq(TSEntry(0, 42, 100)))
    )
  }

  it should "correctly bucket the entries" in {
    // Empty bucket, then an entry spanning the bucket
    assert(
      Seq((1L, Seq()), (11L, Seq(TSEntry(11, 42, 10))), (21L, Seq())) ==
        TimeBucketer.bucketEntries(str, Seq(TSEntry(11, 42, 10)))
    )

    // Empty bucket, then an entry spanning two buckets
    assert(
      Seq((1L, Seq()), (11L, Seq(TSEntry(11, 42, 10))), (21L, Seq(TSEntry(21, 42, 10))), (31L, Seq())) ==
        TimeBucketer.bucketEntries(str, Seq(TSEntry(11, 42, 20)))
    )

    // Entry spanning the first bucket and part of the next one
    assert(
      Seq((1L, Seq(TSEntry(1L, 42, 10))), (11L, Seq(TSEntry(11, 42, 5))), (21L, Seq())) ==
        TimeBucketer.bucketEntries(str, Seq(TSEntry(1, 42, 15)))
    )

    // Entry sitting on the boundary between first two buckets
    assert(
      Seq((1L, Seq(TSEntry(6L, 42, 5))), (11L, Seq(TSEntry(11, 42, 5))), (21L, Seq())) ==
        TimeBucketer.bucketEntries(str, Seq(TSEntry(6, 42, 10)))
    )

    // Two entries in the first bucket
    assert(
      Seq((1L, Seq(TSEntry(1, 42, 5), TSEntry(6, 43, 5))), (11L, Seq())) ==
        TimeBucketer.bucketEntries(str, Seq(TSEntry(1, 42, 5), TSEntry(6, 43, 5)))
    )

    // Two entries in the first bucket, the second extending into the third, with another entry there with a gap
    assert(
      Seq(
        (1L, Seq(TSEntry(1, 42, 5), TSEntry(6, 43, 5))),
        (11L, Seq(TSEntry(11, 43, 5), TSEntry(18, 43, 3))),
        (21L, Seq())
      ) ==
        TimeBucketer.bucketEntries(str, Seq(TSEntry(1, 42, 5), TSEntry(6, 43, 10), TSEntry(18, 43, 3)))
    )

    // Two entries in the first bucket with some spacing,
    // the second extending into the third, with another entry there with a gap
    assert(
      Seq(
        (1L, Seq(TSEntry(2, 42, 4), TSEntry(6, 43, 5))),
        (11L, Seq(TSEntry(11, 43, 5), TSEntry(18, 43, 3))),
        (21L, Seq())
      ) ==
        TimeBucketer.bucketEntries(str, Seq(TSEntry(2, 42, 4), TSEntry(6, 43, 10), TSEntry(18, 43, 3)))
    )
  }
}
