package ch.shastick.immutable

case class TSValue[T](value: T, validity: Long) {
  
  if(validity <= 0) throw new IllegalArgumentException("Validity must be strictly positive")

  /** True if this TSValue is valid at the provided 'atTime' if it is stored at 'key'*/
  def validAt(valueTime: Long, atTime: Long): Boolean = 
    valueTime <= atTime && atTime < valueTime + validity
    
  /** Convert this value to an entry for time 'at' */
  def toEntry(at: Long) = TSEntry(at, value, validity)
  
  /** Trim the validity of this entry, such that 'forTime + updated validity' is less or equal to trimAt
   * 
   * throws an IllegalArgumentException if 'forTime' >= 'trimAt' 
   */
  def trimRight(forTime: Long, trimAt: Long) = 
    if (forTime >= trimAt)
      throw new IllegalArgumentException(
          s"Cannot trim a value's validity (at $trimAt) before that value is supposed to occur ($forTime) on the timeline.");
    else if(forTime + validity <= trimAt) // Nothing to trim
      this
    else 
      TSValue(value, trimAt - forTime)
      
  def map[O](f: T => O) = TSValue(f(value), validity)
}