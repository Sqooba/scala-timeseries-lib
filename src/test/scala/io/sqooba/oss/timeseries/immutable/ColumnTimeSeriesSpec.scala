package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeriesTestBench
import org.scalatest.{FlatSpec, Matchers}

class ColumnTimeSeriesSpec extends FlatSpec with Matchers with TimeSeriesTestBench {

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
