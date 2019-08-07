package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.VectorTimeSeries
import org.scalatest.{FlatSpec, Matchers}

class VectorTimeSeriesSpec extends FlatSpec with Matchers with TimeSeriesTestBench {

  "A VectorTimeSeries" should behave like nonEmptyNonSingletonTimeSeries(
    VectorTimeSeries.ofEntriesUnsafe(_),
    VectorTimeSeries.ofEntriesUnsafe(_)
  )

  // TODO add test for constructor using the 'ofEntriesSafe' function.
}
