package ch.poney

trait TimeSeries[T] {
  
  /** The value valid at time 't' if there is one.*/
  def at(t: Long): Option[T]
  
  /** Split this time series into two.
   *  
   *  Returns a tuple of two contiguous time series,
   *  such that the left time series is never defined for t >= 'at'
   *  and the right time series is never defined for t < 'at'.
   *  
   *  Default implementation simply returns (this.trimRight(at), this.trimLeft(at))
   */
  def split(at: Long): (TimeSeries[T], TimeSeries[T]) = (this.trimRight(at), this.trimLeft(at))
  
  /** Returns a time series that is never defined for t >= at and unchanged for t < at*/
  def trimRight(at: Long): TimeSeries[T]
  
  /** Returns a time series that is never defined for t < at and unchanged for t >= at*/
  def trimLeft(at: Long): TimeSeries[T]
  
  /** The number of elements in this time-series. */
  def size(): Int
  
  /** True if this time series is defined at 'at'. Ie, at('at') would return Some[T] */
  def defined(at: Long): Boolean
}