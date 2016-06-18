package ch.poney.immutable

import ch.poney.TimeSeries

/**
 * TimeSeries implementation based on a Vector.
 *
 * Useful for working on time series like data when no random access is required,
 * as any method requiring some sort of lookup will only run in linear time.
 */
class VectorTimeSeries[T]
  (data: Vector[TSEntry[T]]) // data needs to be SORTED -> TODO: private constructor?
  extends TimeSeries[T] {
  
  /** Linear search for the last element in the time series where the timestamp is less 
   *  or equal to the specified time. */
  def at(t: Long): Option[T] = 
    data(data.lastIndexWhere(_.timestamp <= t)).at(t)

  /** Linear search for the last element in the time series where the timestamp is less 
   *  or equal to the specified time, and returns whether it is valid at time 't' or not. */  
  def defined(t: Long): Boolean = at(t).isDefined

  def map[O](f: T => O): TimeSeries[O] = 
    new VectorTimeSeries(data.map(_.map( f )))

  def size(): Int = data.size

  def trimLeft(t: Long): TimeSeries[T] = 
    if(data.isEmpty) EmptyTimeSeries()
    else
      // TODO: check first and last entry for trivial cases before proceeding?
      data.lastIndexWhere(_.timestamp <= t) match { 
        case -1 => EmptyTimeSeries() // No index where condition holds.
        case i: Int => data.splitAt(i - 1) match {
          case (drop, keep) => drop.last match {
            case e: TSEntry[T] if e.defined(t) => new VectorTimeSeries(e.trimEntryLeft(t) +: keep)
            case _ => new VectorTimeSeries(keep)
          }
      }
    }

  def trimRight(t: Long): TimeSeries[T] =
    if(data.isEmpty) EmptyTimeSeries()
    else
      // TODO: check first and last entry for trivial cases before proceeding?
      data.lastIndexWhere(_.timestamp <= t) match { 
        case -1 => EmptyTimeSeries() // No index where condition holds.
        case i: Int => data.splitAt(i - 1)  match { 
            // First of the last elements is valid and may need trimming. Others can be forgotten.
            case (noChange, lastAndDrop) => 
              new VectorTimeSeries(noChange :+ lastAndDrop.head.trimEntryRight(t))
          }
      }
}

object VectorTimeSeries {
  
  def apply[T](elems: TSEntry[T]*): VectorTimeSeries[T] =
    new VectorTimeSeries(Vector(elems.sortBy(_.timestamp):_*))
  
}