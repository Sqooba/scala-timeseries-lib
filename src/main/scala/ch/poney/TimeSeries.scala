package ch.poney

trait TimeSeries[T] {
  
  /** The value valid at time 't' if there is one.*/
  def at(t: Long): Option[T]
  
  /** The number of elements in this time-series. */
  def size(): Int
}