package io.sqooba.timeseries.immutable

import io.sqooba.timeseries.TimeSeries


/**
  * A time series that is never defined.
  */
case class EmptyTimeSeries[T]() extends TimeSeries[T] {

  def at(t: Long): Option[T] = None

  def size(): Int = 0

  def defined(at: Long) = false

  def trimLeft(at: Long): TimeSeries[T] = EmptyTimeSeries()

  def trimRight(at: Long): TimeSeries[T] = EmptyTimeSeries()

  def map[O](f: T => O): TimeSeries[O] = EmptyTimeSeries()

  def mapWithTime[O](f: (Long, T) => O): TimeSeries[O] = EmptyTimeSeries()

  def fill(whenUndef: T): TimeSeries[T] = EmptyTimeSeries()

  def entries: Seq[TSEntry[T]] = Seq()

  def head: TSEntry[T] = throw new NoSuchElementException()

  def headOption: Option[TSEntry[T]] = None

  def last: TSEntry[T] = throw new NoSuchElementException()

  def lastOption: Option[TSEntry[T]] = None

  def append(other: TimeSeries[T]): TimeSeries[T] = other

  def prepend(other: TimeSeries[T]): TimeSeries[T] = other

  override def resample(sampleLengthMs: Long): TimeSeries[T] = EmptyTimeSeries()
}