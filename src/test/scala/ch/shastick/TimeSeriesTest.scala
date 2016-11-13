package ch.shastick

import org.junit.Test
import org.scalatest.junit.JUnitSuite

import ch.shastick.immutable.TSEntry
import ch.shastick.immutable.TreeMapTimeSeries
import ch.shastick.immutable.EmptyTimeSeries

class TimeSeriesTest extends JUnitSuite {
  
    // Simple summing operator
  def plus(aO: Option[Double], bO: Option[Double]) = 
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a+b)
      case (Some(a), None) => aO
      case (None, Some(b)) => bO
      case _ => None
    }
  
  def mul(aO: Option[Double], bO: Option[Double]): Option[Double] =
    (aO,bO) match {
      case (Some(a), Some(b)) => Some(a.doubleValue * b.doubleValue)
      case (Some(x), None) => Some(x)
      case (None, Some(x)) => Some(x)
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
    
    assert(TimeSeries.mergeEntries(s6)(m6)(plus) ==
      Seq(TSEntry(-10, -1, 10), TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 2.0, 1), TSEntry(6, 4.0, 5), TSEntry(11, 2.0, 5), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5), TSEntry(26, 4.0, 10)))
      
    assert(TimeSeries.mergeEntries(s6)(m6)(plus) ==
      Seq(TSEntry(-10, -1, 10), TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 2.0, 1), TSEntry(6, 4.0, 5), TSEntry(11, 2.0, 5), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5), TSEntry(26, 4.0, 10)))
  
  }
  
  @Test def testContinuousDomainMerges() {
    // Perfectly aligned, no discontinuities
    val l1 = Seq(TSEntry(-20, 1.0, 10), TSEntry(-10, 2.0, 10), TSEntry(0, 3.0, 10), TSEntry(10, 4.0, 10))
    val r1 = Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 6.0, 10), TSEntry(0, 7.0, 10), TSEntry(10, 8.0, 10))
    
    assert(TimeSeries.mergeEntries(l1)(r1)(mul) ==
      Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 12.0, 10), TSEntry(0, 21.0, 10), TSEntry(10, 32.0, 10)))
      
    assert(TimeSeries.mergeEntries(r1)(l1)(mul) ==
      Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 12.0, 10), TSEntry(0, 21.0, 10), TSEntry(10, 32.0, 10)))
      
    // Shifted
    val r2 = r1.map(e => TSEntry(e.timestamp + 5, e.value, e.validity))
      
    assert(TimeSeries.mergeEntries(l1)(r2)(mul) ==
      Seq(TSEntry(-20, 1.0, 5), TSEntry(-15, 5.0, 5), TSEntry(-10, 10.0, 5), TSEntry(-5, 12.0, 5), 
          TSEntry(0, 18.0, 5), TSEntry(5, 21.0, 5), TSEntry(10, 28.0, 5), TSEntry(15, 32.0, 5), TSEntry(20, 8.0, 5)))

    assert(TimeSeries.mergeEntries(r2)(l1)(mul) ==
      Seq(TSEntry(-20, 1.0, 5), TSEntry(-15, 5.0, 5), TSEntry(-10, 10.0, 5), TSEntry(-5, 12.0, 5), 
          TSEntry(0, 18.0, 5), TSEntry(5, 21.0, 5), TSEntry(10, 28.0, 5), TSEntry(15, 32.0, 5), TSEntry(20, 8.0, 5)))
      
    // Denser second sequence
    // Perfectly aligned
    val r3 = Seq(TSEntry(-20, 5.0, 5), TSEntry(-15, 6.0, 5), TSEntry(-10, 7.0, 5), TSEntry(-5, 8.0, 5),
        TSEntry(0, 9.0, 5), TSEntry(5, 10.0, 5), TSEntry(10, 11.0, 5), TSEntry(15, 12.0, 5))
        
    assert(TimeSeries.mergeEntries(l1)(r3)(mul) ==
      Seq(TSEntry(-20, 5.0, 5), TSEntry(-15, 6.0, 5), TSEntry(-10, 14.0, 5), TSEntry(-5, 16.0, 5),
        TSEntry(0, 27.0, 5), TSEntry(5, 30.0, 5), TSEntry(10, 44.0, 5), TSEntry(15, 48.0, 5)))
    
    assert(TimeSeries.mergeEntries(r3)(l1)(mul) ==
      Seq(TSEntry(-20, 5.0, 5), TSEntry(-15, 6.0, 5), TSEntry(-10, 14.0, 5), TSEntry(-5, 16.0, 5),
        TSEntry(0, 27.0, 5), TSEntry(5, 30.0, 5), TSEntry(10, 44.0, 5), TSEntry(15, 48.0, 5)))
      
    // Shifted
    val r4 = r3.map(e => TSEntry(e.timestamp + 4, e.value, e.validity))
    
    assert(TimeSeries.mergeEntries(l1)(r4)(mul) ==
      Seq(TSEntry(-20, 1.0, 4), TSEntry(-16, 5.0, 5), TSEntry(-11, 6.0, 1), TSEntry(-10, 12.0, 4),
        TSEntry(-6, 14.0, 5), TSEntry(-1, 16.0, 1), TSEntry(0, 24.0, 4), TSEntry(4, 27.0, 5),
        TSEntry(9, 30.0, 1), TSEntry(10, 40.0, 4), TSEntry(14, 44.0, 5), TSEntry(19, 48.0, 1),
        TSEntry(20, 12.0, 4)))
        
    assert(TimeSeries.mergeEntries(r4)(l1)(mul) ==
      Seq(TSEntry(-20, 1.0, 4), TSEntry(-16, 5.0, 5), TSEntry(-11, 6.0, 1), TSEntry(-10, 12.0, 4),
        TSEntry(-6, 14.0, 5), TSEntry(-1, 16.0, 1), TSEntry(0, 24.0, 4), TSEntry(4, 27.0, 5),
        TSEntry(9, 30.0, 1), TSEntry(10, 40.0, 4), TSEntry(14, 44.0, 5), TSEntry(19, 48.0, 1),
        TSEntry(20, 12.0, 4)))     
    
  }
  
  @Test def testSlice = {
    val tri = 
       TreeMapTimeSeries(
              0L -> ("Hi", 10L), 
              10L -> ("Ho", 10L),
              20L -> ("Hu", 10L))
    assert(tri.slice(-1, 0) == EmptyTimeSeries())
    assert(tri.slice(-1, 10).entries == Seq(TSEntry(0, "Hi", 10)))
    assert(tri.slice(0, 10).entries == Seq(TSEntry(0, "Hi", 10)))
    assert(tri.slice(0, 9).entries == Seq(TSEntry(0, "Hi", 9)))
    assert(tri.slice(1, 10).entries == Seq(TSEntry(1, "Hi", 9)))
    assert(tri.slice(9, 11).entries == Seq(TSEntry(9, "Hi", 1), TSEntry(10, "Ho", 1)))
    assert(tri.slice(9, 20).entries == Seq(TSEntry(9, "Hi", 1), TSEntry(10, "Ho", 10)))
    assert(tri.slice(10, 20).entries == Seq(TSEntry(10, "Ho", 10)))
    assert(tri.slice(15, 20).entries == Seq(TSEntry(15, "Ho", 5)))
    assert(tri.slice(15, 25).entries == Seq(TSEntry(15, "Ho", 5), TSEntry(20, "Hu", 5)))
    assert(tri.slice(20, 25).entries == Seq(TSEntry(20, "Hu", 5)))
    assert(tri.slice(25, 30).entries == Seq(TSEntry(25, "Hu", 5)))
    assert(tri.slice(25, 35).entries == Seq(TSEntry(25, "Hu", 5)))
    
  }
  
}