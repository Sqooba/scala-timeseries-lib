package ch.poney.immutable

import ch.poney.TimeSeries

case class TSEntry[T]
    (timestamp: Long,  
     value: T,  
     validity: Long) 
     extends TimeSeries[T] {
  
  def at(t: Long): Option[T] = 
    if (t >= timestamp && t < definedUntil) 
      Some(value)
    else 
      None

  def size(): Int = 1
  
  /** Convert this entry to a value */
  lazy val toVal = TSValue(value, validity)
  
  /** Convert this entry to a time->TSVal tuple to be added to a map */
  lazy val toMapTuple = (timestamp -> toVal)
  
  /** Shorten this entry's validity if it exceed 'at'. No effect otherwise.
   *  
   *  If the entry's timestamp is after 'at', the entry remains unchanged.
   */
  def trimRight(at: Long) = 
    if (at <= timestamp) // Trim before or exactly on value start: result is empty.
      EmptyTimeSeries()
    else // Entry needs to have its validity adapted.
      trimEntryRight(at)
      
  /** Similar to trimLeft, but returns a TSEntry instead of a time series and throws 
   *  if 'at' is before the entry's timestamp. */
  def trimEntryRight(at: Long) =
    if (at <= timestamp) // Trim before or exactly on value start: result is empty.
      throw new IllegalArgumentException(
          s"Attempting to trim right at $at before entry's domain has started ($definedUntil)")
    else if (at >= definedUntil) // No effect, return this instance
      this
    else // Entry needs to have its validity adapted.
      TSEntry(timestamp, value, at - timestamp)
  
  /** Move this entry's timestamp to 'at' and shorten the validity accordingly,
   *  if this entry is defined at 'at'. */
  def trimLeft(at: Long) =
    if(at >= definedUntil) // Nothing left from the value on the right side of the trim 
      EmptyTimeSeries()
    else 
      trimEntryLeft(at)
  
  /** Similar to trimLeft, but returns a TSEntry instead of a time series and throws 
   *  if 'at' exceeds the entry's definition. */
  def trimEntryLeft(at: Long) =
    if(at >= definedUntil) // Nothing left from the value on the right side of the trim 
      throw new IllegalArgumentException(
          s"Attempting to trim left at $at after entry's domain has ended ($definedUntil)")
    else if (at <= timestamp) // Trim before or exactly on value start, right side remains unchanged
      this
    else // Entry needs to have its timestamp and validity adapted.
      TSEntry(at, value, definedUntil - at)
  
  def defined(at: Long): Boolean = at >= timestamp && at < definedUntil
  
  /** the last moment where this entry is valid, non-inclusive */
  def definedUntil(): Long = timestamp + validity
      
}

object TSEntry {
  /** Build a TSEntry from a tuple containing the a TSValue and the time at which it starts.*/
  def apply[T](tValTup: (Long, TSValue[T])): TSEntry[T] = 
    TSEntry(tValTup._1, tValTup._2.value, tValTup._2.validity)
    
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