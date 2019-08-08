package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.TSEntry

import scala.collection.mutable

/**
  * Unifies the interface for 'TimeSeriesBuilder' implementations. This enables generic test cases.
  */
// TODO In the future it would be nice to call this trait 'TimeSeriesBuilder'. And the rename the current
//   'TimeSeriesBuilder' to 'VectorTimeSeriesBuilder'.
//   One could also consider to move the builders into the companion objects of the implementations to call them by
//   'VectorTimeSeries.Builder' or 'ColumnTimeSeries.Builder'.

trait TimeSeriesBuilderTrait[T] extends mutable.Builder[TSEntry[T], TimeSeries[T]] {

  /**
    * @return the end of the domain of validity of the last entry added to this builder. None if nothing added yet.
    */
  def definedUntil: Option[Long]
}
