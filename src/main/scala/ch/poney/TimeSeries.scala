package ch.poney

import ch.poney.immutable.TSEntry
import scala.util.Left
import scala.annotation.tailrec

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
  
  /** Map the values within the time series. 
   *  Timestamps and validities of entries remain unchanged*/
  def map[O](f: T => O): TimeSeries[O]
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
  def mergeEntries[A,B,C]
    (a: Seq[TSEntry[A]])
    (b: Seq[TSEntry[B]])
    (op: (Option[A], Option[B]) => Option[C])
    : Seq[TSEntry[C]] = {
    mergeEithers(Seq.empty)((a.map(_.toLeftEntry[B]) ++ b.map(_.toRightEntry[A])).sortBy(_.timestamp))(op)
  }
  
  @tailrec
  def mergeEithers[A,B,C]
    (done: Seq[TSEntry[C]]) // 
    (todo: Seq[TSEntry[Either[A,B]]])
    (op: (Option[A], Option[B]) => Option[C])
    : Seq[TSEntry[C]] = 
      todo match {
        case Seq() => // Nothing remaining, we are done -> return the merged Seq 
          done 
        case Seq(head, remaining @_*) => 
          // Take the head and all entries with which it overlaps and merge them.
          // Remaining entries are merged via a recursive call
          val (toMerge, nextRound) = 
            remaining.span(_.timestamp < head.definedUntil()) match {
            case (vals :+ last, r) if last.defined(head.definedUntil) =>
              // we need to add the part that is defined after the head to the 'nextRound' entries
              (vals :+ last, last.trimEntryLeft(head.definedUntil) +: r)
            case t: Any => t
          }
          // Check if there was some empty space between the last 'done' entry and the first remaining
          val filling = done.lastOption match {
            case Some(TSEntry(ts, valE, d)) => 
              if (ts + d == head.timestamp) // Continuous domain, no filling to do
                Seq.empty
              else
                op(None,None).map(TSEntry(ts + d, _, head.timestamp)).toSeq
            case _ => Seq.empty
          }
          val p = TSEntry.mergeSingleToMultiple(head, toMerge)(op)
          // Add the freshly merged entries to the previously done ones, call to self with the remaining entries.
          mergeEithers(done ++ filling ++ TSEntry.mergeSingleToMultiple(head, toMerge)(op))(nextRound)(op)
      }
  
}
