package io.sqooba.timeseries.immutable

import io.sqooba.timeseries.TimeSeries

/**
  * A time series that is never defined.
  */
case class EmptyTimeSeries[T]() extends TimeSeries[T] {

  def at(t: Long): Option[T] = None

  def size(): Int = 0

  def defined(at: Long): Boolean = false

  def trimLeft(at: Long): TimeSeries[T] = this

  def trimRight(at: Long): TimeSeries[T] = this

  def map[O](f: T => O, compress: Boolean = true): TimeSeries[O] = EmptyTimeSeries[O]()

  def filter(predicate: TSEntry[T] => Boolean): TimeSeries[T] = this

  def filterValues(predicate: T => Boolean): TimeSeries[T] = this

  def mapWithTime[O](f: (Long, T) => O, compress: Boolean = true): TimeSeries[O] = EmptyTimeSeries[O]()

  def fill(whenUndef: T): TimeSeries[T] = this

  def entries: Seq[TSEntry[T]] = Seq()

  def head: TSEntry[T] = throw new NoSuchElementException()

  def headOption: Option[TSEntry[T]] = None

  def headValue: T = throw new NoSuchElementException()

  def headValueOption: Option[T] = None

  def last: TSEntry[T] = throw new NoSuchElementException()

  def lastOption: Option[TSEntry[T]] = None

  def lastValue: T = throw new NoSuchElementException()

  def lastValueOption: Option[T] = None

  def append(other: TimeSeries[T]): TimeSeries[T] = other

  def prepend(other: TimeSeries[T]): TimeSeries[T] = other

  def resample(sampleLengthMs: Long): TimeSeries[T] = this

  override def looseDomain: Option[LooseDomain] = None
}
