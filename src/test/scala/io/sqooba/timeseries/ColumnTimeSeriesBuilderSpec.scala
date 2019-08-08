package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{ColumnTimeSeries, TSEntry}
import org.scalatest.{FlatSpec, Matchers}

class ColumnTimeSeriesBuilderSpec extends FlatSpec with Matchers with TimeSeriesBuilderTestBench {

  private def newColumnBuilder(compress: Boolean) = new ColumnTimeSeriesBuilder[Int](compress)

  "A ColumnTimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    newColumnBuilder(true).vectorResult() shouldBe (Vector(), Vector(), Vector())
  }

  it should "be a ColumnTimeSeries if at least two non equal elements are added" in {
    val b = newColumnBuilder(true)
    val data = Vector(
      TSEntry(1, 1, 1),
      TSEntry(3, 3, 3)
    )

    b ++= data
    val result = b.result()
    result.entries should contain theSameElementsInOrderAs data
    result.isInstanceOf[ColumnTimeSeries[_]] shouldBe true
  }

  it should behave like aTimeSeriesBuilder(newColumnBuilder)
}
