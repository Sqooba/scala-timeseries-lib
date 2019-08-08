package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{TSEntry, VectorTimeSeries}
import org.scalatest.{FlatSpec, Matchers}

class TimeSeriesBuilderSpec extends FlatSpec with Matchers with TimeSeriesBuilderTestBench {

  private def newTsb = new TimeSeriesBuilder[Int]

  "A TimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    newTsb.vectorResult() shouldBe Vector()
  }

  it should "be a VectorTimeSeries if at least two non equal elements are added" in {
    val b = newTsb
    val data = Vector(
      TSEntry(1, 1, 1),
      TSEntry(3, 3, 3)
    )

    b ++= data
    val result = b.result()
    result.entries should contain theSameElementsInOrderAs data
    result.isInstanceOf[VectorTimeSeries[_]] shouldBe true
  }

  it should behave like aTimeSeriesBuilder(compress => new TimeSeriesBuilder[Int](compress))
}
