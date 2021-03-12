package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeriesTestBench
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class ColumnTimeSeriesSpec extends AnyFlatSpec with Matchers with TimeSeriesTestBench {

  "A ColumnTimeSeries" should behave like nonEmptyNonSingletonDoubleTimeSeries(
        ColumnTimeSeries.ofOrderedEntriesSafe[Double](_)
      )

  it should behave like nonEmptyNonSingletonGenericTimeSeries(
        ColumnTimeSeries.ofOrderedEntriesSafe[String](_)
      )

  it should behave like nonEmptyNonSingletonDoubleTimeSeriesWithCompression(
        ColumnTimeSeries.ofOrderedEntriesSafe[Double](_)
      )
}
