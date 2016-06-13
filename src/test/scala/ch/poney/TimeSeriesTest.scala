package ch.poney

import org.junit.Test
import org.scalatest.junit.JUnitSuite
import ch.poney.immutable.TSEntry

class TimeSeriesTest extends JUnitSuite {
  
    // Simple summing operator
  def plus(aO: Option[Double], bO: Option[Double]) = 
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a+b)
      case (Some(a), None) => aO
      case (None, Some(b)) => bO
      case _ => None
    }

  @Test def testSeqMergingSingleToMultiple() {
    // Single to single within domain.
    val s1 = Seq(TSEntry(1, 2.0, 20))
    val m1 = Seq(TSEntry(5, 1.0, 10))
    
    assert(TimeSeries.mergeEntries(s1)(m1)(plus) ==
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6)))
    
    assert(TimeSeries.mergeEntries(m1)(s1)(plus) ==
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6)))
    
    
    // Merging with two entries wholly contained in the single's domain
    val s3 = Seq(TSEntry(1, 2.0, 20))
    val m3 = Seq(TSEntry(5, 1.0, 5), TSEntry(10, 2.0, 5))
    
    assert(TimeSeries.mergeEntries(s3)(m3)(plus) ==
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6)))
      
    assert(TimeSeries.mergeEntries(m3)(s3)(plus) ==
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6)))
      
    val s4 = Seq(TSEntry(1, 2.0, 20))
    val m4 = Seq(TSEntry(5, 1.0, 5), TSEntry(11, 2.0, 5))
    
    assert(TimeSeries.mergeEntries(s4)(m4)(plus) ==
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5)))
      
    assert(TimeSeries.mergeEntries(m4)(s4)(plus) ==
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5)))
    
    // Merge with three entries, the first and last one exceeding the single's domain
    val s5 = Seq(TSEntry(1, 2.0, 20))
    val m5 = Seq(TSEntry(0, 1.0, 5), TSEntry(5, 2.0, 5), TSEntry(16, 3.0, 10))
    
    assert(TimeSeries.mergeEntries(s5)(m5)(plus) ==
      Seq(TSEntry(0,1.0,1), TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5)))
      
    assert(TimeSeries.mergeEntries(m5)(s5)(plus) ==
      Seq(TSEntry(0,1.0,1), TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5)))
    
    // Merge with four entries, the first and last one being completely outside of the single's domain
    val s6 = Seq(TSEntry(1, 2.0, 20))
    val m6 = Seq(TSEntry(-10, -1.0, 10), TSEntry(0, 1.0, 5), TSEntry(6, 2.0, 5), TSEntry(16, 3.0, 10), TSEntry(26, 4.0, 10))
    println(6)
    assert(TimeSeries.mergeEntries(s6)(m6)(plus) ==
      Seq(TSEntry(-10, -1, 10), TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 2.0, 1), TSEntry(6, 4.0, 5), TSEntry(11, 2.0, 5), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5), TSEntry(26, 4.0, 10)))
      
    assert(TimeSeries.mergeEntries(s6)(m6)(plus) ==
      Seq(TSEntry(-10, -1, 10), TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 2.0, 1), TSEntry(6, 4.0, 5), TSEntry(11, 2.0, 5), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5), TSEntry(26, 4.0, 10)))
  
  }
  
}