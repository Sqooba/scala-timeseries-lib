package ch.shastick

import org.junit.Test
import org.scalatest.junit.JUnitSuite

import ch.shastick.immutable.TreeMapTimeSeries
import ch.shastick.immutable.TSEntry

/**
 * Assumes the merge logic to be well tested: 
 * just check that the general numerical operations
 * are implemented correctly.
 */
class NumericTimeSeriesTest extends JUnitSuite {
  
   val tsa = TreeMapTimeSeries(
        1L -> (1.0, 10L),
        // Leave a gap of 1 between the two entries
        12L -> (2.0, 10L))  
        
   val tsb = TreeMapTimeSeries(
        6L -> (3.0, 10L))

  /**
   * Check that we only have a correct sum wherever 
   * both time series are defined at the same time.
   */
  @Test def testStrictPlus {
    
    assert(tsa.plus(tsb).entries == tsb.plus(tsa).entries)
    
    assert(Seq(TSEntry(6, 4.0, 5), TSEntry(12, 5.0, 4))
        == tsa.plus(tsb).entries)
  }
  
  /**
   * Check that we only have a correct subtraction wherever 
   * both time series are defined at the same time.
   */
  @Test def testStrictMinus {
    
    assert(Seq(TSEntry(6, -2.0, 5), TSEntry(12, -1.0, 4))
        == tsa.minus(tsb).entries)
    
    assert(Seq(TSEntry(6, 2.0, 5), TSEntry(12, 1.0, 4))
        == tsb.minus(tsa).entries)
  }
  
  /**
   * Check that we only have a correct multiplication wherever 
   * both time series are defined at the same time.
   */
  @Test def testStrictMultiply {
    
  assert(tsa.multiply(tsb).entries == tsb.multiply(tsa).entries)
    
    assert(Seq(TSEntry(6, 3.0, 5), TSEntry(12, 6.0, 4))
        == tsb.multiply(tsa).entries)
  }
  
  
}