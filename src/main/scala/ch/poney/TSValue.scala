package ch.poney

case class TSValue[T](value: T, validity: Long) {

  /** True if this TSValue is valid at the provided 'atTime' if it is stored at 'key'*/
  def validFor(key: Long, atTime: Long): Boolean = 
    key <= atTime && atTime <= key + validity
    
  /** Convert this value to an entry for time 'at' */
  def toEntry(at: Long) = TSEntry(at, value, validity)
}