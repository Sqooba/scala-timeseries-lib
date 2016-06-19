package ch.poney.immutable

import ch.poney.TimeSeries

/**
 * A time series that is never defined.
 */
case class EmptyTimeSeries[T]() extends TimeSeries[T] {
  
  def at(t: Long): Option[T] = None
  
  val size = 0
  
  def defined(at: Long) = false

  def trimLeft(at: Long): TimeSeries[T] = EmptyTimeSeries()

  def trimRight(at: Long): TimeSeries[T] = EmptyTimeSeries()
  
  def map[O](f: T => O): TimeSeries[O] = EmptyTimeSeries()
  
  def entries: Seq[TSEntry[T]] = Seq()
}