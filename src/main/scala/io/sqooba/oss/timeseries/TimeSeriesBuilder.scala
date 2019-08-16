package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.mutable

/**
  * Unifies the interface for builders of timeseries implementations. This enables generic test cases.
  */
// TODO: can we make this trait more useful and hence reduce the boilerplate needed in each individual builder?
trait TimeSeriesBuilder[T] extends mutable.Builder[TSEntry[T], TimeSeries[T]] {

  /**
    * @return the end of the domain of validity of the last entry added to this builder. None if nothing added yet.
    */
  def definedUntil: Option[Long]
}
