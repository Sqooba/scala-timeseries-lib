package ch.poney

class TSEntry[T]
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
      
  
}