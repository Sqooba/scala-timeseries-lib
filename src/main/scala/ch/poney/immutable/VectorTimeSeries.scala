package ch.poney.immutable

import ch.poney.TimeSeries

/**
 * TimeSeries implementation based on a Vector.
 *
 * Useful for working on time series like data when no random access is required,
 * as any method requiring some sort of lookup will only run in linear time.
 */
case class VectorTimeSeries[T]
  (data: Vector[TSEntry[T]]) // data needs to be SORTED -> TODO: private constructor?
  extends TimeSeries[T] {
  
  /** Linear search for the last element in the time series where the timestamp is less 
   *  or equal to the specified time. */
  def at(t: Long): Option[T] = 
    data.lastIndexWhere(_.timestamp <= t) match {
      case -1 => None
      case i: Int => data(i).at(t)
    }

  /** Linear search for the last element in the time series where the timestamp is less 
   *  or equal to the specified time, and returns whether it is valid at time 't' or not. */  
  def defined(t: Long): Boolean = at(t).isDefined

  def map[O](f: T => O): TimeSeries[O] = 
    new VectorTimeSeries(data.map(_.map( f )))

  def size(): Int = data.size

  def trimLeft(t: Long): TimeSeries[T] =
    // Check obvious shortcuts
    if(data.isEmpty) 
      EmptyTimeSeries()
    else if(data.size == 1) 
      data.head.trimLeft(t)
    else if(data.head.timestamp >= t)
      this
    else
      data.lastIndexWhere(_.timestamp <= t) match { 
        case -1 => EmptyTimeSeries() // No index where condition holds.
        case i: Int => data.splitAt(i) match {
          case (drop, keep) => keep.head match {
            case e: TSEntry[T] if e.defined(t) => 
              new VectorTimeSeries(e.trimEntryLeft(t) +: keep.tail)
            case _ =>  
              if (keep.size == 1) 
                EmptyTimeSeries()
              else
                new VectorTimeSeries(keep.tail)
          }
        }
    }

  def trimRight(t: Long): TimeSeries[T] =
    if(data.isEmpty) 
      EmptyTimeSeries()
    else if(data.size == 1)
      data.head.trimRight(t)
      // TODO: check last entry for trivial cases before proceeding?
    else
      data.lastIndexWhere(_.timestamp < t) match { 
        case -1 => EmptyTimeSeries() // No index where condition holds.
        case 0 => data.head.trimRight(t) //First element: trim and return it.  
        case i: Int => data.splitAt(i)  match { 
            // First of the last elements is valid and may need trimming. Others can be forgotten.
            case (noChange, lastAndDrop) =>
              new VectorTimeSeries(noChange :+ lastAndDrop.head.trimEntryRight(t))
          }
      }

  def entries: Seq[TSEntry[T]] = data

  def head: TSEntry[T] = data.head

  def headOption: Option[TSEntry[T]] = data.headOption

  def last: TSEntry[T] = data.last

  def lastOption: Option[TSEntry[T]] = data.lastOption

  def append(other: TimeSeries[T]): TimeSeries[T] = 
    other.headOption match {
      case None => // other is empty, nothing to do.
        this
      case Some(tse) if tse.timestamp > head.definedUntil => // Something to keep from the current TS
        VectorTimeSeries.ofEntries(this.trimRight(tse.timestamp).entries ++ other.entries)
      case _ => // Nothing to keep, other overwrites this TS completely 
        other
    }

  def prepend(other: TimeSeries[T]): TimeSeries[T] = 
    other.lastOption match {
      case None => // other is empty, nothing to do.
        this
      case Some(tse) if tse.timestamp < last.definedUntil => // Something to keep from the current TS
        VectorTimeSeries.ofEntries(other.entries ++ this.trimLeft(tse.definedUntil).entries)
      case _ => // Nothing to keep, other overwrites this TS completely
        other
    }
  
}

object VectorTimeSeries {
  
  def ofEntries[T](elems: Seq[TSEntry[T]]): VectorTimeSeries[T] =
    // TODO: Expect entries to be sorted and just check?
    new VectorTimeSeries(Vector(elems.sortBy(_.timestamp):_*))
  
  def apply[T](elems: (Long, (T, Long))*): VectorTimeSeries[T] =
    ofEntries(elems.map(t => TSEntry(t._1, t._2._1, t._2._2)));
  
}