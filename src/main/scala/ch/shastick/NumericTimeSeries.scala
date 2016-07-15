package ch.shastick

object NumericTimeSeries {
  
  /** 
   *  Defensive 'plus' operator: wherever a time series 
   *  is  undefined, the result is undefined.
   */
  def safePlus[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
    : Option[T] = {
      import n._
      (lhO, rhO) match {
        case (Some(l), Some(r)) => Some(l + r)
        case _ => None
      }
  }
  
  /** 
   *  Defensive 'minus' operator: wherever a time series 
   *  is  undefined, the result is undefined.
   */
  def safeMinus[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
    : Option[T] = { 
      import n._
      (lhO, rhO) match {
        case (Some(l), Some(r)) => Some(l - r)
        case _ => None
      }
  }
}