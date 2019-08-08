package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.ColumnTimeSeries
import org.scalatest.{FlatSpec, Matchers}

class ColumnTimeSeriesSpec extends FlatSpec with Matchers with TimeSeriesTestBench {

  it should behave like nonEmptyNonSingletonTimeSeries(
    ColumnTimeSeries.ofOrderedEntriesSafe[String](_),
    ColumnTimeSeries.ofOrderedEntriesSafe[Double](_)
  )
}
