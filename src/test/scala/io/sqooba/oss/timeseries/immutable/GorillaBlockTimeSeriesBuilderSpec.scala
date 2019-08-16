package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeriesBuilderTestBench
import org.scalatest.{FlatSpec, Matchers}

class GorillaBlockTimeSeriesBuilderSpec extends FlatSpec with Matchers with TimeSeriesBuilderTestBench {

  "A GorillaTimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    GorillaBlockTimeSeries.newBuilder().result() shouldBe EmptyTimeSeries
  }

  it should "be a GorillaTimeSeries if at least two non equal elements are added" in {
    val b = GorillaBlockTimeSeries.newBuilder()
    val data = Vector(
      TSEntry(1, 1d, 1),
      TSEntry(3, 3d, 3)
    )

    b ++= data
    val result = b.result()
    result.entries should contain theSameElementsInOrderAs data
    result.isInstanceOf[GorillaBlockTimeSeries] shouldBe true
  }

  it should behave like aTimeSeriesBuilder(GorillaBlockTimeSeries.newBuilder)
}
