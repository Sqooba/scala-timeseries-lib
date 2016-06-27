package ch.poney.immutable

import ch.poney.TimeSeries
import scala.collection.immutable.TreeMap

case class TreeMapTimeSeries[T]
  (data: TreeMap[Long, TSValue[T]])
  extends TimeSeries[T] {
  
  def at(t: Long): Option[T] = 
    data.to(t) match {
      case m: Map[_,_] if m.isEmpty => None
      case m: Map[_,_] => 
        data.get(m.lastKey)
          .filter(_.validAt(m.lastKey, t))
          .map(_.value)
  }

  def size(): Int = data.size

  def defined(t: Long): Boolean = at(t).isDefined

  def trimLeft(at: Long): TimeSeries[T] = 
    data.to(at).lastOption
      .map(TSEntry(_)) match {// Get the last entry on the left of the trim spot
      case Some(entry) => 
        if (entry.defined(at)) {
          // If it's still valid, we need to save the part right from the trim spot
          TreeMapTimeSeries(data.from(at) + (at -> entry.trimEntryLeft(at).toVal))
        } else {
          // If it's not valid anymore, we can safely forget about it.
          data.from(at) match {
            // Check if there actually is anything left right of the split point
            case m: Map[_,_] =>
              if (m.isEmpty)
                EmptyTimeSeries()
              else
                TreeMapTimeSeries(m);
            }
        }
      case None => // Nothing to trim, return as is. 
        this
    }

  def trimRight(at: Long): TimeSeries[T] =
    // cut at 'at' - 1, as any entry located at exactly 'at' would need to be trimmed away as well.
    data.to(at - 1) match {
      case m: Map[_,_] if m.isEmpty => // Nothing to return 
        EmptyTimeSeries()
      case m: Map[_,_] =>
        // Potentially entries to remove. Trim the last element before the split and return it along
        // all the previous elements.
          TreeMapTimeSeries(
            data.to(m.lastKey - 1) + TSEntry(m.last).trimEntryRight(at).toMapTuple)
    }
    
  def map[O](f: T => O): TimeSeries[O] =
    new TreeMapTimeSeries(data.map(t => (t._1, t._2.map(f))))

  def entries: Seq[TSEntry[T]] = data.map(e => TSEntry(e)).toSeq

  def head: TSEntry[T] = TSEntry(data.head)

  def headOption: Option[TSEntry[T]] = data.headOption.map(TSEntry(_))

  def last: TSEntry[T] = TSEntry(data.last)

  def lastOption: Option[TSEntry[T]] = data.lastOption.map(TSEntry(_))

  def append(other: TimeSeries[T]): TimeSeries[T] = 
    other.headOption match {
      case None => // other is empty, nothing to do.
        this
      case Some(tse) if tse.timestamp > head.definedUntil => // Something to keep from the current TS
        TreeMapTimeSeries.ofEntries(this.trimRight(tse.timestamp).entries ++ other.entries)
      case _ => // Nothing to keep, other overwrites this TS completely 
        other
    }

  def prepend(other: TimeSeries[T]): TimeSeries[T] = 
    other.lastOption match {
      case None => // other is empty, nothing to do.
        this
      case Some(tse) if tse.timestamp < last.definedUntil => // Something to keep from the current TS
        TreeMapTimeSeries.ofEntries(other.entries ++ this.trimLeft(tse.definedUntil).entries)
      case _ => // Nothing to keep, other overwrites this TS completely
        other
    }
  
}

object TreeMapTimeSeries {
  
  def ofEntries[T](elems: Seq[TSEntry[T]]): TimeSeries[T] =
    apply(elems.map(_.toMapTuple):_*)
  
  def apply[T](elems: (Long, TSValue[T])*): TimeSeries[T] = 
    if (elems.isEmpty)
      EmptyTimeSeries()
    else 
      new TreeMapTimeSeries(TreeMap(elems:_*))
  
}