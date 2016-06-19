package ch.poney

import org.junit.Test
import org.scalatest.junit.JUnitSuite
import ch.poney.immutable.VectorTimeSeries
import ch.poney.immutable.EmptyTimeSeries
import ch.poney.immutable.TSEntry

class VectorTimeSeriesTest extends JUnitSuite  {
  
  val empty = VectorTimeSeries()
  
  val single = VectorTimeSeries((1L -> ("Hi", 10L)))
  
  val contig2 = VectorTimeSeries((1L -> ("Hi", 10L)), 
                                (11L -> ("Ho", 10L)))
                                
  val discon2 = VectorTimeSeries((1L -> ("Hi", 10L)), 
                                (12L -> ("Ho", 10L)))

  @Test def testAtNSize() {
    // Check empty
    assert(0 == empty.size())
    assert(None == empty.at(0))
    
    // Check single value
    assert(1 == single.size())
    assert(None == single.at(0))
    assert(Some("Hi") == single.at(1))
    assert(Some("Hi") == single.at(10))
    assert(None == single.at(11))
    
    // Check two contiguous values
    assert(2 == contig2.size())
    assert(None == contig2.at(0))
    assert(Some("Hi") == contig2.at(1))
    assert(Some("Hi") == contig2.at(10))
    assert(Some("Ho") == contig2.at(11))
    assert(Some("Ho") == contig2.at(20))
    assert(None == contig2.at(21))
    
    // Check two non contiguous values
    assert(2 == discon2.size())
    assert(None == discon2.at(0))
    assert(Some("Hi") == discon2.at(1))
    assert(Some("Hi") == discon2.at(10))
    assert(None == discon2.at(11))
    assert(Some("Ho") == discon2.at(12))
    assert(Some("Ho") == discon2.at(21))
    assert(None == discon2.at(22))
    
  }
  
  @Test def testDefined() {
    // Check empty
    assert(!empty.defined(0))
    
    // Check single value
    assert(!single.defined(0))
    assert(single.defined(1))
    assert(single.defined(10))
    assert(!single.defined(11))
    
    // Check two contiguous values
    assert(!contig2.defined(0))
    assert(contig2.defined(1))
    assert(contig2.defined(10))
    assert(contig2.defined(11))
    assert(contig2.defined(20))
    assert(!contig2.defined(21))
    
    // Check two non contiguous values
    assert(!discon2.defined(0))
    assert(discon2.defined(1))
    assert(discon2.defined(10))
    assert(!discon2.defined(11))
    assert(discon2.defined(12))
    assert(discon2.defined(21))
    assert(!discon2.defined(22))
  }
  
  @Test def testTrimLeft() {
    
    // Trimming an empty TS:
    assert(EmptyTimeSeries() == empty.trimLeft(-1))
    
    // Single entry
    // Trimming left of the domain
    assert(single == single.trimLeft(0))
    assert(single == single.trimLeft(1))
    
    // Trimming an entry
    assert(TSEntry(2, "Hi", 9) == single.trimLeft(2))
    assert(TSEntry(10, "Hi", 1) == single.trimLeft(10))
    assert(EmptyTimeSeries() == single.trimLeft(11))
    
    // Two contiguous entries
    // Left of the domain
    assert(contig2 == contig2.trimLeft(0))
    assert(contig2 == contig2.trimLeft(1))
    
    // Trimming in the first entry
    assert(Seq(TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10)) == contig2.trimLeft(2))
    assert(Seq(TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10)) == contig2.trimLeft(10))
  }
  
  @Test def testTrimRight() {
    ???
  }
  
  @Test def testSplit() {
    ???
  }
  
  @Test def testMap() {
    ???
  }
}