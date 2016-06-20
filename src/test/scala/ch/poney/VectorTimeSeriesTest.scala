package ch.poney

import org.junit.Test
import org.scalatest.junit.JUnitSuite
import ch.poney.immutable.VectorTimeSeries
import ch.poney.immutable.EmptyTimeSeries
import ch.poney.immutable.TSEntry

class VectorTimeSeriesTest extends JUnitSuite  {
  
  val empty = VectorTimeSeries()
  
  // Single entry
  val single = VectorTimeSeries(1L -> ("Hi", 10L))
  
  // Two contiguous entries
  val contig2 = VectorTimeSeries(1L -> ("Hi", 10L), 
                                11L -> ("Ho", 10L))
                                
  // Two entries with a gap in between            
  val discon2 = VectorTimeSeries(1L -> ("Hi", 10L), 
                                12L -> ("Ho", 10L))
                   
  // Three entries, gap between first and second
  val three = VectorTimeSeries(1L -> ("Hi", 10L), 
                              12L -> ("Ho", 10L),
                              22L -> ("Ha", 10L))

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
  
  @Test def testTrimLeftContiguous() {
    
    // Trimming an empty TS:
    assert(EmptyTimeSeries() == empty.trimLeft(-1))
    
    // Single entry
    // Trimming left of the domain
    assert(single.entries.head == single.trimLeft(0))
    assert(single.entries.head == single.trimLeft(1))
    
    // Trimming an entry
    assert(TSEntry(2, "Hi", 9) == single.trimLeft(2))
    assert(TSEntry(10, "Hi", 1) == single.trimLeft(10))
    assert(EmptyTimeSeries() == single.trimLeft(11))
    
    // Two contiguous entries
    // Left of the domain
    assert(contig2 == contig2.trimLeft(0))
    assert(contig2 == contig2.trimLeft(1))
    
    // Trimming on the first entry
    assert(Seq(TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10)) == contig2.trimLeft(2).entries)
    assert(Seq(TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10)) == contig2.trimLeft(10).entries)
    
    // Trimming at the boundary between entries:
    assert(Seq(TSEntry(11, "Ho", 10)) == contig2.trimLeft(11).entries)
    
    // ... and on the second entry:
    assert(Seq(TSEntry(12, "Ho", 9)) == contig2.trimLeft(12).entries)
    assert(Seq(TSEntry(20, "Ho", 1)) == contig2.trimLeft(20).entries)
    
    // ... and after the second entry:
    assert(EmptyTimeSeries() == contig2.trimLeft(21))
    
  }
  
  @Test def testTrimLeftDiscontinuous() {
    // Two non-contiguous entries
    // Trimming left of the first entry
    assert(discon2 == discon2.trimLeft(0))
    assert(discon2 == discon2.trimLeft(1))
    
    // Trimming on the first entry
    assert(Seq(TSEntry(2, "Hi", 9), TSEntry(12, "Ho", 10)) == discon2.trimLeft(2).entries)
    assert(Seq(TSEntry(10, "Hi", 1), TSEntry(12, "Ho", 10)) == discon2.trimLeft(10).entries)
    
    // Trimming between entries:
    assert(Seq(TSEntry(12, "Ho", 10)) == discon2.trimLeft(11).entries)
    assert(Seq(TSEntry(12, "Ho", 10)) == discon2.trimLeft(12).entries)
    
    // ... and on the second
    assert(Seq(TSEntry(13, "Ho", 9)) == discon2.trimLeft(13).entries)
    assert(Seq(TSEntry(21, "Ho", 1)) == discon2.trimLeft(21).entries)
    
    // ... and after the second entry:
    assert(EmptyTimeSeries() == discon2.trimLeft(22))
    
    
    // Trim on a three element time series with a discontinuity
    // Left of the first entry
    assert(three == three.trimLeft(0))
    assert(three == three.trimLeft(1))
      
    // Trimming on the first entry
    assert(Seq(TSEntry(2, "Hi", 9), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) == 
      three.trimLeft(2).entries)
    assert(Seq(TSEntry(10, "Hi", 1), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) == 
      three.trimLeft(10).entries)
    
    // Trimming between entries:
    assert(Seq(TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) == three.trimLeft(11).entries)
    assert(Seq(TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) == three.trimLeft(12).entries)
    
    // ... and on the second
    assert(Seq(TSEntry(13, "Ho", 9), TSEntry(22, "Ha", 10)) == three.trimLeft(13).entries)
    assert(Seq(TSEntry(21, "Ho", 1), TSEntry(22, "Ha", 10)) == three.trimLeft(21).entries)
    
    // ... on the border between second and third
    assert(Seq(TSEntry(22, "Ha", 10)) == three.trimLeft(22).entries)
    // on the third
    assert(Seq(TSEntry(23, "Ha", 9)) == three.trimLeft(23).entries)
    assert(Seq(TSEntry(31, "Ha", 1)) == three.trimLeft(31).entries)
    
    //... and after every entry.
    assert(EmptyTimeSeries() == three.trimLeft(32))
  }
  
  @Test def testTrimRightContiguous() {
    // empty case...
    assert(EmptyTimeSeries() == empty.trimRight(0))
    
    // Single entry:
    // Right of the domain:
    assert(single.entries.head == single.trimRight(12))
    assert(single.entries.head == single.trimRight(11))
    
    // On the entry
    assert(TSEntry(1, "Hi", 9) == single.trimRight(10))
    assert(TSEntry(1, "Hi", 1) == single.trimRight(2))
    
    // Left of the entry
    assert(EmptyTimeSeries() == single.trimRight(1))
    assert(EmptyTimeSeries() == single.trimRight(0))
    
    // Two contiguous entries:
    // Right of the domain:
    assert(contig2 == contig2.trimRight(22))
    assert(contig2 == contig2.trimRight(21))
    
    // On the second entry
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9)) == contig2.trimRight(20).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1)) == contig2.trimRight(12).entries)
    
    // On the boundary
    assert(Seq(TSEntry(1, "Hi", 10)) == contig2.trimRight(11).entries)
    
    // On the first entry
    assert(TSEntry(1, "Hi", 9) == contig2.trimRight(10))
    assert(TSEntry(1, "Hi", 1) == contig2.trimRight(2))
    
    // Before the first entry
    assert(EmptyTimeSeries() == contig2.trimRight(1))
    assert(EmptyTimeSeries() == contig2.trimRight(0))
    
  }
  
  @Test def testTrimRightDiscontinuous() {
    // Two non-contiguous entries
    // Trimming right of the second entry
    assert(discon2 == discon2.trimRight(23))
    assert(discon2 == discon2.trimRight(22))
    
    // Trimming on the last entry
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 9)) == discon2.trimRight(21).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 1)) == discon2.trimRight(13).entries)
    
    // Trimming between entries:
    assert(Seq(TSEntry(1, "Hi", 10)) == discon2.trimRight(12).entries)
    assert(Seq(TSEntry(1, "Hi", 10)) == discon2.trimRight(11).entries)
    
    // ... and on the first
    assert(Seq(TSEntry(1, "Hi", 9)) == discon2.trimRight(10).entries)
    assert(Seq(TSEntry(1, "Hi", 1)) == discon2.trimRight(2).entries)
    
    // ... and before the first entry:
    assert(EmptyTimeSeries() == discon2.trimRight(1))
    assert(EmptyTimeSeries() == discon2.trimRight(0))
    
    
    // Trim on a three element time series with a discontinuity
    // Right of the last entry
    assert(three == three.trimRight(33))
    assert(three == three.trimRight(32))
    
    // Trimming on the last entry
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 9)) == 
      three.trimRight(31).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 1)) == 
      three.trimRight(23).entries)
       
    // Trimming between 2nd and 3rd entries:
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10)) == three.trimRight(22).entries)
    
    // ... and on the second
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 9)) == three.trimRight(21).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 1)) == three.trimRight(13).entries)
    
    // ... on the border between 1st and 2nd
    assert(Seq(TSEntry(1, "Hi", 10)) == three.trimRight(12).entries)
    assert(Seq(TSEntry(1, "Hi", 10)) == three.trimRight(11).entries)
    
    // ... on the first
    assert(Seq(TSEntry(1, "Hi", 9)) == three.trimRight(10).entries)
    assert(Seq(TSEntry(1, "Hi", 1)) == three.trimRight(2).entries)
    
    //... and after every entry.
    assert(EmptyTimeSeries() == three.trimRight(1))
    assert(EmptyTimeSeries() == three.trimRight(0))
  }
  
  @Test def testSplit() {
    val tri = 
       VectorTimeSeries(
              0L -> ("Hi", 10L), 
              10L -> ("Ho", 10L),
              20L -> ("Hu", 10L))
    assert(tri.split(-1) == (EmptyTimeSeries(), tri))
    assert(tri.split(0) == (EmptyTimeSeries(), tri))
    assert(tri.split(1) == (tri.trimRight(1), tri.trimLeft(1)))
    assert(tri.split(9) == (tri.trimRight(9), tri.trimLeft(9)))
    assert(tri.split(10) == (tri.trimRight(10), tri.trimLeft(10)))
    assert(tri.split(11) == (tri.trimRight(11), tri.trimLeft(11)))
    assert(tri.split(19) == (tri.trimRight(19), tri.trimLeft(19)))
    assert(tri.split(20) == (tri.trimRight(20), tri.trimLeft(20)))
    assert(tri.split(21) == (tri.trimRight(21), tri.trimLeft(21)))
    assert(tri.split(29) == (tri.trimRight(29), tri.trimLeft(29)))
    assert(tri.split(30) == (tri,EmptyTimeSeries()))
    assert(tri.split(31) == (tri,EmptyTimeSeries()))
  }
  
  @Test def testMap() {
    val tri = 
       VectorTimeSeries(
              0L -> ("Hi", 10L), 
              10L -> ("Ho", 10L),
              20L -> ("Hu", 10L))
              
    val up = tri.map(s => s.toUpperCase())
    assert(3 == up.size())
    assert(up.at(0) == Some("HI"))
    assert(up.at(10) == Some("HO"))
    assert(up.at(20) == Some("HU"))
  }
}