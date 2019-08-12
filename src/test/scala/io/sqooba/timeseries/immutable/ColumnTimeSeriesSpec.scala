package io.sqooba.timeseries.immutable

import io.sqooba.timeseries.TimeSeriesTestBench
import org.scalatest.{FlatSpec, Matchers}

class ColumnTimeSeriesSpec extends FlatSpec with Matchers with TimeSeriesTestBench {

  it should behave like nonEmptyNonSingletonTimeSeries(
    ColumnTimeSeries.ofOrderedEntriesSafe[String](_),
    ColumnTimeSeries.ofOrderedEntriesSafe[Double](_)
  )
}
