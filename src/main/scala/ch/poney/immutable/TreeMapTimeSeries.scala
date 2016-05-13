package ch.poney.immutable

import ch.poney.TimeSeries
import scala.collection.immutable.TreeMap
import ch.poney.TSValue

class TreeMapTimeSeries[T]
  (data: TreeMap[Long, TSValue[T]])
  extends TimeSeries[T] {
  
  def at(t: Long): Option[T] = 
    data.to(t) match {
      case m: Map[_,_] if m.isEmpty => None
      case m: Map[_,_] => 
        data.get(m.lastKey)
          .filter(_.validFor(m.lastKey, t))
          .map(_.value)
  }

  def size(): Int = data.size
   
}