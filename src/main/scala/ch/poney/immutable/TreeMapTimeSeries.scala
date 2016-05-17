package ch.poney.immutable

import ch.poney.TimeSeries
import scala.collection.immutable.TreeMap

class TreeMapTimeSeries[T]
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

  def trimLeft(at: Long): TimeSeries[T] = {
    data.to(at).lastOption
      .map(TSEntry(_)) match {// Get the last entry on the left of the trim spot
      case Some(entry) => 
        if (entry.defined(at)) 
          // If it's still valid, we need to save the part right from the trim spot
          new TreeMapTimeSeries(data.from(at) + (at -> entry.trimEntryLeft(at).toVal))
        else
          // If it's not valid anymore, we can safely forget about it.
          new TreeMapTimeSeries(data.from(at))
      case None => // Nothing to trim, return as is. 
        this
    }
  }

  def trimRight(at: Long): TimeSeries[T] = {
    data.to(at) match {
      case m: Map[_,_] if m.isEmpty => // Nothing to return 
        EmptyTimeSeries()
      case m: Map[_,_] if m.size == data.size => 
        // No entries to remove, but the last one might need a trim.
        new TreeMapTimeSeries(
            data.to(m.lastKey - 1) + TSEntry(m.last).trimEntryRight(at).toMapTuple)
    }
  }
  
  
  
   
}