package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeriesTestBench
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class GorillaBlockTimeSeriesSpec extends AnyFlatSpec with Matchers with TimeSeriesTestBench {

  private val entries = Seq(TSEntry(1, 1.2, 2), TSEntry(10, -2d, 2), TSEntry(11, 1d, 1))

  "GorillaBlockTimeSeries" should "choose its builder type by the type of the result" in {

    val series = GorillaBlockTimeSeries.ofOrderedEntriesSafe(entries)
    val gorillaBuilder = series.newBuilder[Double]()

    gorillaBuilder ++= entries
    gorillaBuilder.result() shouldBe a[GorillaBlockTimeSeries]

    val otherBuilder = series.newBuilder[Any]()
    otherBuilder ++= Seq(TSEntry(1, "hi", 20), TSEntry(2, 1324, 20))
    otherBuilder.result() shouldBe a[VectorTimeSeries[_]]
  }

  it should "choose its result implementation by the result type of a map" in {
    val series = GorillaBlockTimeSeries.ofOrderedEntriesSafe(
      Seq(TSEntry(1, 1d, 1), TSEntry(2, 4d, 1))
    )

    series.map[Double](_ * 2) shouldBe a[GorillaBlockTimeSeries]

    series.map(_ => "hello", compress = false) should not be a[GorillaBlockTimeSeries]
    series.map(_ => "hello", compress = false) shouldBe a[VectorTimeSeries[_]]

    series.map(_ => "hello") shouldBe a[TSEntry[_]]
  }

  it should behave like nonEmptyNonSingletonDoubleTimeSeries(
        GorillaBlockTimeSeries.ofOrderedEntriesSafe(_)
      )

  it should behave like nonEmptyNonSingletonDoubleTimeSeriesWithCompression(
        GorillaBlockTimeSeries.ofOrderedEntriesSafe(_)
      )

  it should "have entries that are traversable multiple times" in {
    val inputEntries = Seq(TSEntry(1, 1d, 1), TSEntry(2, 4d, 1))
    val series = GorillaBlockTimeSeries.ofOrderedEntriesSafe(inputEntries)

    series.entries.isTraversableAgain shouldBe true

    val es = series.entries
    noException should be thrownBy {
      es.toVector shouldBe es.toVector
      es.toSeq shouldBe inputEntries
    }
  }
}
