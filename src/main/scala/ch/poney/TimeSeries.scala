package ch.poney

import ch.poney.immutable.TSEntry

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

object TimeSeries {
  
  /** Merge two time series together, using the provided merge operator.
   *  
   *  The passed TSEntry sequences will be merged according to the merge operator,
   *  which will always be applied to one of the following:
   *    - two defined TSEntries with exactly the same domain of definition
   *    - a defined entry from A and None from B
   *    - a defined entry from B and None from A
   *    - No defined entry from A nor B.
   *    
   *  Overlapping TSEntries in the sequences a and b are trimmed to fit
   *  one of the aforementioned cases before being passed to the merge function.
   *  
   *  For example, 
   *    - if 'x' and '-' respectively represent the undefined and defined parts of a TSEntry 
   *    - '|' delimits the moment on the time axis where a change in definition occurs either 
   *       in the present entry or in the one with which it is currently being merged
   *    - 'result' is the sequence resulting from the merge
   *    
   *    We apply the merge function in the following way:
   *  
   *  a_i:    xxx|---|---|xxx|xxx
   *  b_j:    xxx|xxx|---|---|xxx
   *  
   *  result: (1) (2) (3) (4) (5)
   *  
   *  (1),(5) : op(None, None)
   *  (2) : op(Some(a_i.value), None)
   *  (3) : op(Some(a_i.value), Some(b_j.value))
   *  (4) : op(None, Some(b_j.value))
   *  
   *  Assumes a and b to be ORDERED!
   */
  def merge[A,B,C]
    (op: (Option[A], Option[B]) => Option[C])
    (a: Seq[TSEntry[A]])
    (b: Seq[TSEntry[B]])
    : Seq[TSEntry[C]] = ???
  
}