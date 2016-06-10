package ch.poney

import org.junit.Test
import org.scalatest.junit.JUnitSuite
import ch.poney.immutable.TSEntry
import ch.poney.immutable.TSValue
import ch.poney.immutable.EmptyTimeSeries

class TSEntryTest extends JUnitSuite {
  
  @Test def testAt() {
    assert(!TSEntry(0, "", 10).at(-1).isDefined)
    assert(TSEntry(0, "", 10).at(0) == Some(""))
    assert(TSEntry(0, "", 10).at(9) == Some(""))
    assert(!TSEntry(0, "", 10).at(10).isDefined)
  }
  
  @Test def testDefined() {
    assert(!TSEntry(0, "", 10).defined(-1))
    assert(TSEntry(0, "", 10).defined(0))
    assert(TSEntry(0, "", 10).defined(9))
    assert(!TSEntry(0, "", 10).defined(10))
  }
  
  @Test def testDefinedUntil() {
    assert(TSEntry(1, "", 10).definedUntil() == 11)
  }
  
  @Test def testToMapTuple() {
    assert(TSEntry(0, "Hi", 10).toMapTuple == (0 -> TSValue("Hi",10)))
  }
  
  @Test def testToVal() {
    assert(TSEntry(0, "Hi", 10).toVal == TSValue("Hi",10))
  }
  
  @Test def testTrimRight() {
    val tse = TSEntry(0, "", 10)
    assert(tse.trimRight(10) == tse)
    assert(tse.trimRight(9) == TSEntry(0, "", 9))
    assert(tse.trimRight(1) == TSEntry(0, "", 1))
    assert(tse.trimRight(0) == EmptyTimeSeries())
    assert(tse.trimRight(-1) == EmptyTimeSeries())
  }
  
  @Test def testTrimEntryRight() {
    val tse = TSEntry(0, "", 10)
    assert(tse.trimEntryRight(10) == tse)
    assert(tse.trimEntryRight(9) == TSEntry(0, "", 9))
    assert(tse.trimEntryRight(1) == TSEntry(0, "", 1))
    
    intercept[IllegalArgumentException] {
      tse.trimEntryRight(0)
    }
    intercept[IllegalArgumentException] {
      tse.trimEntryRight(-1)
    }
  }
  
  @Test def testTrimLeft() {
    val tse = TSEntry(1, "", 10)
    assert(tse.trimLeft(0) == tse)
    assert(tse.trimLeft(1) == tse)
    assert(tse.trimLeft(2) == TSEntry(2, "", 9))
    assert(tse.trimLeft(10) == TSEntry(10, "", 1))
    assert(tse.trimLeft(11) == EmptyTimeSeries())
    assert(tse.trimLeft(12) == EmptyTimeSeries())
  }
  
  @Test def testTrimEntryLeft() {
    val tse = TSEntry(1, "", 10)
    assert(tse.trimEntryLeft(0) == tse)
    assert(tse.trimEntryLeft(1) == tse)
    assert(tse.trimEntryLeft(2) == TSEntry(2, "", 9))
    assert(tse.trimEntryLeft(10) == TSEntry(10, "", 1))
    intercept[IllegalArgumentException] {
      tse.trimEntryLeft(11)
    }
    intercept[IllegalArgumentException] {
      tse.trimEntryLeft(12)
    }
  }
  
  @Test def testSplit() {
    val tse = TSEntry(0, "", 10)
    assert(tse.split(0) == (EmptyTimeSeries(), tse))
    assert(tse.split(1) == (tse.trimEntryRight(1), tse.trimEntryLeft(1)))
    assert(tse.split(5) == (tse.trimEntryRight(5), tse.trimEntryLeft(5)))
    assert(tse.split(9) == (tse.trimEntryRight(9), tse.trimEntryLeft(9)))
    assert(tse.split(10) == (tse,EmptyTimeSeries()))
  }
  
  @Test def testMergeEntriesSimpleOp() {
    
    //Simple summing operator when both entries are defined. 
    def op(aO: Option[Double], bO: Option[Double]) = 
      (aO, bO) match {
        case (Some(a), Some(b)) => Some(a+b)
        case (Some(a), None) => aO
        case (None, Some(b)) => bO
        case _ => None
      }
    
    // For two exactly overlapping entries,
    // result contains a single entry
    val r1 = TSEntry.merge(TSEntry(1, 2.0, 10), TSEntry(1, 3.0, 10))(op)
    assert(r1.size == 1)
    assert(r1(0) == TSEntry(1, 5.0, 10))   
    
    // Entries don't start at the same time, but have the same end of validity
    val a = TSEntry(1, 2.0, 10)
    val b = TSEntry(6, 3.0, 5)
    assert(a.definedUntil() == b.definedUntil())
    
    // Result should contain two entries: first a, valid until b starts, then the sum.
    val r2 = TSEntry.merge(a,b)(op) 
    assert(r2.size == 2)
    assert(r2(0) == TSEntry(1, 2.0, 5))
    assert(r2(1) == TSEntry(6, 5.0, 5))
    
    // Should be the same if we inverse the inputs...
    val r3 = TSEntry.merge(b,a)(op) 
    assert(r3.size == 2)
    assert(r3(0) == TSEntry(1, 2.0, 5))
    assert(r3(1) == TSEntry(6, 5.0, 5))
    
    // Entries start at the same time, but have different end of validity
    val a4 = TSEntry(1, 2.0, 10)
    val b4 = TSEntry(1, 3.0, 5)
    val r4 = TSEntry.merge(a4,b4)(op)
    assert(r4.size == 2)
    assert(r4(0) == TSEntry(1, 5.0, 5))
    assert(r4(1) == TSEntry(6, 2.0, 5))
    
    val r5 = TSEntry.merge(b4,a4)(op)
    assert(r5.size == 2)
    assert(r5(0) == TSEntry(1, 5.0, 5))
    assert(r5(1) == TSEntry(6, 2.0, 5))
    
    // Two overlapping entries that don't share bounds for their domain of definition:
    // Should result in three different TSEntries
    val a6 = TSEntry(1, 2.0, 10)
    val b6 = TSEntry(6, 3.0, 10)
    val r6 = TSEntry.merge(a6, b6)(op)
    assert(r6.size == 3)
    assert(r6(0) == TSEntry(1, 2.0, 5))
    assert(r6(1) == TSEntry(6, 5.0, 5))
    assert(r6(2) == TSEntry(11, 3.0, 5))
    
    // Same for inverted input...
    val r7 = TSEntry.merge(b6, a6)(op)
    assert(r7.size == 3)
    assert(r7(0) == TSEntry(1, 2.0, 5))
    assert(r7(1) == TSEntry(6, 5.0, 5))
    assert(r7(2) == TSEntry(11, 3.0, 5))
    
    // Finally, check that non-overlapping entries lead to a seq containing them as-is.
    // obviously not overlapping:
    val r8 = TSEntry.merge(TSEntry(1, 2.0, 10), TSEntry(12, 3.0, 10))(op)
    assert(r8.size == 2)
    assert(r8(0) == TSEntry(1, 2.0, 10))
    assert(r8(1) == TSEntry(2, 3.0, 10))
    
    // contiguous but not overlapping: remain untouched as well
    val r9 =  TSEntry.merge(TSEntry(1, 2.0, 10), TSEntry(11, 3.0, 10))(op)
    assert(r9.size == 2)
    assert(r9(0) == TSEntry(1, 2.0, 10))
    assert(r9(1) == TSEntry(2, 3.0, 10))
  }
  
  @Test def testTrimLeftNRight() {
    ???
  }
  
  @Test def testOverlaps() {
    ???
  }
}