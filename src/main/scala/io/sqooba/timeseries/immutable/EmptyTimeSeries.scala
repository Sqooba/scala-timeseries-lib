package io.sqooba.timeseries.immutable

import io.sqooba.timeseries.TimeSeries

/**
  * A time series that is never defined.
  */
case object EmptyTimeSeries extends TimeSeries[Nothing] {

  def at(t: Long): Option[Nothing] = None

  def size(): Int = 0

  def defined(at: Long): Boolean = false

  def trimLeft(at: Long): TimeSeries[Nothing] = this

  def trimRight(at: Long): TimeSeries[Nothing] = this

  def map[O](f: Nothing => O, compress: Boolean = true): TimeSeries[O] = this

  def filter(predicate: TSEntry[Nothing] => Boolean): TimeSeries[Nothing] = this

  def filterValues(predicate: Nothing => Boolean): TimeSeries[Nothing] = this

  def mapWithTime[O](f: (Long, Nothing) => O, compress: Boolean = true): TimeSeries[O] = this

  def fill[U >: Nothing](whenUndef: U): TimeSeries[U] = this

  def entries: Seq[TSEntry[Nothing]] = Seq()

  def head: TSEntry[Nothing] = throw new NoSuchElementException()

  def headOption: Option[TSEntry[Nothing]] = None

  def headValue: Nothing = throw new NoSuchElementException()

  def headValueOption: Option[Nothing] = None

  def last: TSEntry[Nothing] = throw new NoSuchElementException()

  def lastOption: Option[TSEntry[Nothing]] = None

  def lastValue: Nothing = throw new NoSuchElementException()

  def lastValueOption: Option[Nothing] = None

  def append[U >: Nothing](other: TimeSeries[U]): TimeSeries[U] = other

  def prepend[U >: Nothing](other: TimeSeries[U]): TimeSeries[U] = other

  def resample(sampleLengthMs: Long): TimeSeries[Nothing] = this

  def looseDomain: TimeDomain = EmptyTimeDomain
}
