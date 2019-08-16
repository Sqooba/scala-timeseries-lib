package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeriesTestBench
import org.scalatest.{FlatSpec, Matchers}

class VectorTimeSeriesSpec extends FlatSpec with Matchers with TimeSeriesTestBench {

  "A VectorTimeSeries" should behave like nonEmptyNonSingletonDoubleTimeSeries(
    VectorTimeSeries.ofOrderedEntriesUnsafe(_)
  )

  it should behave like nonEmptyNonSingletonGenericTimeSeries(
    VectorTimeSeries.ofOrderedEntriesUnsafe(_)
  )

  // TODO add test for constructor using the 'ofEntriesSafe' function.
}
