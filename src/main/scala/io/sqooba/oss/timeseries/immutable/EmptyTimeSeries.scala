package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeries

import scala.reflect.runtime.universe._

/**
  * A time series that is never defined.
  */
case object EmptyTimeSeries extends TimeSeries[Nothing] {

  def at(t: Long): Option[Nothing] = None

  def entryAt(t: Long): Option[TSEntry[Nothing]] = None

  def size: Int = 0

  def isEmpty: Boolean = true

  def isCompressed: Boolean = false

  def trimLeft(at: Long): TimeSeries[Nothing] = this

  def trimLeftDiscrete(at: Long, includeEntry: Boolean): TimeSeries[Nothing] = this

  def trimRight(at: Long): TimeSeries[Nothing] = this

  def trimRightDiscrete(at: Long, includeEntry: Boolean): TimeSeries[Nothing] = this

  def mapWithTime[O: WeakTypeTag](f: (Long, Nothing) => O, compress: Boolean = true): TimeSeries[O] = this

  def filter(predicate: TSEntry[Nothing] => Boolean): TimeSeries[Nothing] = this

  override def fill[U >: Nothing](whenUndef: U): TimeSeries[U] = this

  def entries: Seq[TSEntry[Nothing]] = Seq()

  override def values: Seq[Nothing] = Seq()

  def headOption: Option[TSEntry[Nothing]] = None

  def lastOption: Option[TSEntry[Nothing]] = None

  override def splitEntriesLongerThan(sampleLengthMs: Long): TimeSeries[Nothing] = this

  def looseDomain: TimeDomain = EmptyTimeDomain

  def supportRatio: Double = 0

  def isDomainContinuous: Boolean = false
}
