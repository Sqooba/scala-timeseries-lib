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
    : Seq[TSEntry[C]] = {
    
    a.head.timestamp < b.head.timestamp
   ???
  }
  
  /** Merge two overlapping TSEntries and return the result as an
   *  ordered sequence of TSEntries. 
   *  
   *  Assumes the two entries have at least an overlapping domain of definition.
   *    
   *  This method returns a Seq containing one to three TSEntries defining a timeseries valid from
   *  first.timestamp to max(first.validUntil, second.validUntil).
   *    - one entry if first and second share the exact same domain
   *    - two entries if first and second share one bound of their domain
   *    - three entries if the domains overlap without sharing a bound
   */
  def mergeEntries[A,B,R]
    (op: (Option[A], Option[B]) => Option[R])
    (a: TSEntry[A], b: TSEntry[B])
    : Seq[TSEntry[R]] = 
    {    
      // Handle first 'partial' definition 
      (Math.min(a.timestamp, b.timestamp), Math.max(a.timestamp, b.timestamp)) match {
      case (from, to) if from == to => Seq.empty // a and b start at the same time. Nothing to do
      case (from, to) => 
        // Compute the result of the merge operation for a partially defined input (either A or B is undefined for this segment)
        mergeValues(op)(a,b)(from,to)
      }
    } ++ {  
      // Merge the two values over the overlapping domain of definition of a and b.
      (Math.max(a.timestamp, b.timestamp), Math.min(a.definedUntil, b.definedUntil)) match {
      case (from, to) if from > to => throw new IllegalArgumentException("Entries' domain of definition must overlap.")
      case (from, to) => mergeValues(op)(a,b)(from,to)
      }
    } ++ {
      // Handle trailing 'partial' definition
      (Math.min(a.definedUntil(), b.definedUntil()), Math.max(a.definedUntil(), b.definedUntil())) match {
        case (from, to) if from == to => Seq.empty; // Entries end at the same time, nothing to do.
        case (from, to) => mergeValues(op)(a,b)(from,to)
      }
    }
  
  /** Convenience function to merge the values present in the entries at time 'at' and
   *  create an entry valid until 'until' from the result, if the merge operation is defined
   *  for the input.
   */
  def mergeValues[A, B, R]
    (op: (Option[A], Option[B]) => Option[R])
    (a: TSEntry[A], b: TSEntry[B])
    (at: Long, until: Long)
    : Seq[TSEntry[R]] = 
      op(a.at(at), b.at(at)).map(TSEntry(at, _ , until - at)).toSeq
  
}