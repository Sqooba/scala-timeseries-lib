package io.sqooba.timeseries.immutable

import io.sqooba.timeseries.{ColumnTimeSeriesBuilder, TimeSeries, TimeSeriesBuilder}

import scala.annotation.tailrec

/**
  * TimeSeries implementation based on a column-store of three Vectors, one for each field of TSEntry.
  * This implementation should be more memory-efficient than the vanilla VectorTimeSeries.
  *
  * @note the entries of the three vectors must be ordered in the same way. The element at the same index in the three
  *       of them represents a TSEntry.
  */
case class ColumnTimeSeries[+T] private[timeseries] (
    timestamps: Vector[Long],
    values: Vector[T],
    validities: Vector[Long],
    isCompressed: Boolean = false,
    isDomainContinuous: Boolean = false
) extends TimeSeries[T] {

  require(
    timestamps.size >= 2,
    "A ColumnTimeSeries can not be empty (should be an EmptyTimeSeries) nor contain only one element (should be a TSEntry)"
  )

  require(
    timestamps.size == values.size && values.size == validities.size,
    "All three column vectors need to have the same number of elements."
  )

  def entries: Seq[TSEntry[T]] = (timestamps, values, validities).zipped.map(TSEntry[T])

  def at(t: Long): Option[Nothing] = ???

  def entryAt(t: Long): Option[TSEntry[Nothing]] = ???

  def size: Int = 0

  def isEmpty: Boolean = ???

  def defined(at: Long): Boolean = ???

  def trimLeft(at: Long): TimeSeries[Nothing] = ???

  def trimLeftDiscrete(at: Long, includeEntry: Boolean): TimeSeries[Nothing] = ???

  def trimRight(at: Long): TimeSeries[Nothing] = ???

  def trimRightDiscrete(at: Long, includeEntry: Boolean): TimeSeries[Nothing] = ???

  def map[O](f: T => O, compress: Boolean = true): TimeSeries[O] = ???

  def filter(predicate: TSEntry[T] => Boolean): TimeSeries[Nothing] = ???

  def filterValues(predicate: T => Boolean): TimeSeries[Nothing] = ???

  def mapWithTime[O](f: (Long, T) => O, compress: Boolean = true): TimeSeries[O] = ???

  def fill[U >: Nothing](whenUndef: U): TimeSeries[U] = ???

  def head: TSEntry[Nothing] = ???

  def headOption: Option[TSEntry[Nothing]] = ???

  def headValue: Nothing = ???

  def headValueOption: Option[Nothing] = ???

  def last: TSEntry[Nothing] = ???

  def lastOption: Option[TSEntry[Nothing]] = ???

  def lastValue: Nothing = ???

  def lastValueOption: Option[Nothing] = ???

  def resample(sampleLengthMs: Long): TimeSeries[Nothing] = ???

  def looseDomain: TimeDomain = ???

  def supportRatio: Double = 0
}
