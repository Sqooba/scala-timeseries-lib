package ch.poney.immutable

import ch.poney.TimeSeries

case class TSEntry[T]
    (timestamp: Long,  
     value: T,  
     validity: Long) 
     extends TimeSeries[T] {
  
  def at(t: Long): Option[T] = 
    if (t >= timestamp && t <= timestamp + validity) 
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
  
  def defined(at: Long): Boolean = at >= timestamp && at < definedUntil
  
  /** the last moment where this entry is valid, non-inclusive */
  def definedUntil(): Long = timestamp + validity
      
}

object TSEntry {
  /** Build a TSEntry from a tuple containing the a TSValue and the time at which it starts.*/
  def apply[T](tValTup: (Long, TSValue[T])): TSEntry[T] = 
    TSEntry(tValTup._1, tValTup._2.value, tValTup._2.validity)
}