package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.scalatest.{FlatSpec, Matchers}

class TimeSeriesBuilderSpec extends FlatSpec with Matchers with TimeSeriesBuilderTestBench {

  "A TimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    TimeSeries.newBuilder().result() shouldBe EmptyTimeSeries
  }

  it should "be a VectorTimeSeries if at least two non equal elements are added" in {
    val b = TimeSeries.newBuilder[Int]()
    val data = Vector(
      TSEntry(1, 1, 1),
      TSEntry(3, 3, 3)
    )

    b ++= data
    val result = b.result()
    result.entries should contain theSameElementsInOrderAs data
    result shouldBe a[VectorTimeSeries[_]]
  }

  it should behave like aTimeSeriesBuilder(TimeSeries.newBuilder)
}
