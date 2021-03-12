package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeriesBuilderTestBench
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class ColumnTimeSeriesBuilderSpec extends AnyFlatSpec with Matchers with TimeSeriesBuilderTestBench {

  "A ColumnTimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    ColumnTimeSeries.newBuilder().result() shouldBe EmptyTimeSeries
  }

  it should "be a ColumnTimeSeries if at least two non equal elements are added" in {
    val b = ColumnTimeSeries.newBuilder[Int]()
    val data = Vector(
      TSEntry(1, 1, 1),
      TSEntry(3, 3, 3)
    )

    b ++= data
    val result = b.result()
    result.entries should contain theSameElementsInOrderAs data
    result shouldBe a[ColumnTimeSeries[_]]
  }

  it should behave like aTimeSeriesBuilder(ColumnTimeSeries.newBuilder)
}
