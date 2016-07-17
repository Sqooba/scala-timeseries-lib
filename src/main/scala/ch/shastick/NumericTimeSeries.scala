package ch.shastick

object NumericTimeSeries {
  
  /** 
   *  Defensive 'plus' operator: wherever one of the time series 
   *  is  undefined, the result is undefined.
   */
  def strictPlus[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
    : Option[T] = {
      import n._
      (lhO, rhO) match {
        case (Some(l), Some(r)) => Some(l + r)
        case _ => None
      }
  }
  
  /** 
   *  Defensive 'minus' operator: wherever one of the time series 
   *  is  undefined, the result is undefined.
   */
  def strictMinus[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
    : Option[T] = { 
      import n._
      (lhO, rhO) match {
        case (Some(l), Some(r)) => Some(l - r)
        case _ => None
      }
  }
  
  /** 
   *  Defensive multiplication operator: wherever one of the time series
   *  is  undefined, the result is undefined.
   */
  def strictMultiply[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
    : Option[T] = { 
      import n._
      (lhO, rhO) match {
        case (Some(l), Some(r)) => Some(l * r)
        case _ => None
      }
  }
}