package ch.shastick

import org.junit.Test
import org.scalatest.junit.JUnitSuite

import org.junit.Assert._
import ch.shastick.immutable.TreeMapTimeSeries
import ch.shastick.immutable.TSEntry
import scala.collection.immutable.TreeMap
import ch.shastick.immutable.TreeMapTimeSeries
import ch.shastick.immutable.TSValue
import ch.shastick.immutable.EmptyTimeSeries

class TreeMapTimeSeriesTest extends JUnitSuite {
  
  @Test def testTreeMapTSReads() {
    // Check empty case...
    val empty = new TreeMapTimeSeries(TreeMap())
    assert(0 == empty.size)
    
    // With one entry
    val single = 
      new TreeMapTimeSeries(
        TreeMap(0L -> TSValue("Hi", 10)))
    assert(1 == single.size)
    assert(!single.at(-1).isDefined)
    assert(single.at(0).isDefined)
    assert(single.at(9).isDefined)
    assert(!single.at(10).isDefined)
    
    // With two entries
    val double = 
      new TreeMapTimeSeries(
          TreeMap(
              0L -> TSValue("Hi", 10), 
              10L -> TSValue("Ho", 10)))
    assert(2 == double.size)
    assert(!double.at(-1).isDefined)
    assert(double.at(0).get == "Hi")
    assert(double.at(9).get == "Hi")
    assert(double.at(10).get == "Ho")
    assert(double.at(19).get == "Ho")
    assert(!double.at(20).isDefined)
  }
  
  @Test def testDefined() {
   
    val double = 
      TreeMapTimeSeries(
              0L -> ("Hi", 10L), 
              20L -> ("Ho", 10L))

    assert(!double.defined(-1))
    assert(double.defined(0))
    assert(double.defined(9))
    assert(!double.defined(10))
    
    assert(!double.defined(19))
    assert(double.defined(20))
    assert(double.defined(29))
    assert(!double.defined(30))
  }
  
  @Test def testTrimLeft() {
    
    // Check empty case...
    assert(TreeMapTimeSeries().trimLeft(0) == EmptyTimeSeries())
    
    // With one entry
    val single = 
      TreeMapTimeSeries(
        1L -> ("Hi", 10L))
        
    assert(single.trimLeft(1) == single)
    assert(single.trimLeft(2) ==  
      TreeMapTimeSeries(2L -> ("Hi", 9L)))
      
    assert(single.trimLeft(10) ==  
      TreeMapTimeSeries(10L -> ("Hi", 1L)))
      
    assert(single.trimLeft(11) ==  EmptyTimeSeries())
    assert(single.trimLeft(12) ==  EmptyTimeSeries())
    
    // With two entries
    val double = 
      TreeMapTimeSeries(
              1L -> ("Hi", 10L), 
              11L -> ("Ho", 10L))
              
    assert(double.trimLeft(1) == double)
    assert(double.trimLeft(2) ==  
      TreeMapTimeSeries(
          2L -> ("Hi", 9L),
          11L -> ("Ho", 10L)))
      
    assert(double.trimLeft(10) ==  
      TreeMapTimeSeries( 
          10L -> ("Hi", 1L),
          11L -> ("Ho", 10L)))
      
    assert(double.trimLeft(11) ==  
      TreeMapTimeSeries( 
          11L -> ("Ho", 10L)))
          
    assert(double.trimLeft(12) ==  
      TreeMapTimeSeries( 
          12L -> ("Ho", 9L)))
          
    assert(double.trimLeft(20) ==  
      TreeMapTimeSeries( 
          20L -> ("Ho", 1L)))
          
   assert(double.trimLeft(21) ==  EmptyTimeSeries())
   assert(double.trimLeft(22) ==  EmptyTimeSeries())
  }
  
  @Test def testTrimRight() {
        // Check empty case...
    assert(TreeMapTimeSeries().trimRight(0) == EmptyTimeSeries())
    
    // With one entry
    val single = 
      TreeMapTimeSeries(
        1L -> ("Hi", 10L))
        
    assert(single.trimRight(11) == single)
    assert(single.trimRight(10) ==  
      TreeMapTimeSeries( 1L -> ("Hi", 9L)))
      
    assert(single.trimRight(2) ==  
      TreeMapTimeSeries( 1L -> ("Hi", 1L)))
      
    assert(single.trimRight(1) ==  EmptyTimeSeries())
    assert(single.trimRight(0) ==  EmptyTimeSeries())
    
    // With two entries
    val double = 
      TreeMapTimeSeries(
              1L -> ("Hi", 10L), 
              11L -> ("Ho", 10L))
              
    assert(double.trimRight(21) == double)
    assert(double.trimRight(20) ==  
      TreeMapTimeSeries(
          1L -> ("Hi", 10L),
          11L -> ("Ho", 9L)))
      
    assert(double.trimRight(12) ==  
      TreeMapTimeSeries( 
          1L -> ("Hi", 10L),
          11L -> ("Ho", 1L)))
      
    assert(double.trimRight(11) ==  
      TreeMapTimeSeries( 
          1L -> ("Hi", 10L)))
          
    assert(double.trimRight(10) ==  
      TreeMapTimeSeries( 
          1L -> ("Hi", 9L)))
          
    assert(double.trimRight(2) ==  
      TreeMapTimeSeries( 
          1L -> ("Hi", 1L)))
          
   assert(double.trimRight(1) ==  EmptyTimeSeries())
   assert(double.trimRight(0) ==  EmptyTimeSeries())
  }
  
  @Test def testSplit() {
    val tri = 
       TreeMapTimeSeries(
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
       TreeMapTimeSeries(
              0L -> ("Hi", 10L), 
              10L -> ("Ho", 10L),
              20L -> ("Hu", 10L))
              
    val up = tri.map(s => s.toUpperCase())
    assert(3 == up.size())
    assert(up.at(0) == Some("HI"))
    assert(up.at(10) == Some("HO"))
    assert(up.at(20) == Some("HU"))
  }
  
  @Test def appendEntry() {
    val tri = 
       TreeMapTimeSeries(
              1L -> ("Hi", 10L), 
              11L -> ("Ho", 10L),
              21L -> ("Hu", 10L))
     
    // Appending after...
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10), TSEntry(32, "Hy", 10))
        == tri.append(TSEntry(32, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10), TSEntry(31, "Hy", 10))
        == tri.append(TSEntry(31, "Hy", 10)).entries)
        
    // Appending on last entry
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 9), TSEntry(30, "Hy", 10))
        == tri.append(TSEntry(30, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 1), TSEntry(22, "Hy", 10))
        == tri.append(TSEntry(22, "Hy", 10)).entries)
        
    // ... just after and on second entry
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hy", 10))
        == tri.append(TSEntry(21, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9), TSEntry(20, "Hy", 10))
        == tri.append(TSEntry(20, "Hy", 10)).entries)
    
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1), TSEntry(12, "Hy", 10))
        == tri.append(TSEntry(12, "Hy", 10)).entries)
        
    // ... just after and on first
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Hy", 10))
        == tri.append(TSEntry(11, "Hy", 10)).entries)
    
    assert(Seq(TSEntry(1, "Hi", 9), TSEntry(10, "Hy", 10))
        == tri.append(TSEntry(10, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 1), TSEntry(2, "Hy", 10))
        == tri.append(TSEntry(2, "Hy", 10)).entries)
        
    // And complete override
    assert(Seq(TSEntry(1, "Hy", 10))
        == tri.append(TSEntry(1, "Hy", 10)).entries)
        
  }
  
  @Test def prependEntry() {
    val tri = 
       TreeMapTimeSeries(
              1L -> ("Hi", 10L), 
              11L -> ("Ho", 10L),
              21L -> ("Hu", 10L))
              
    // Prepending before...
    assert(Seq(TSEntry(-10, "Hy", 10), TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(-10, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(-9, "Hy", 10), TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(-9, "Hy", 10)).entries)
    
    // Overlaps with first entry
    assert(Seq(TSEntry(-8, "Hy", 10), TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(-8, "Hy", 10)).entries)
    
    assert(Seq(TSEntry(0, "Hy", 10), TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(0, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(1, "Hy", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(1, "Hy", 10)).entries)
        
    // ... second entry
    assert(Seq(TSEntry(2, "Hy", 10), TSEntry(12, "Ho", 9), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(2, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(10, "Hy", 10), TSEntry(20, "Ho", 1), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(10, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(11, "Hy", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(11, "Hy", 10)).entries)
    
    // ... third entry
    assert(Seq(TSEntry(12, "Hy", 10), TSEntry(22, "Hu", 9))
        == tri.prepend(TSEntry(12, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(20, "Hy", 10), TSEntry(30, "Hu", 1))
        == tri.prepend(TSEntry(20, "Hy", 10)).entries)
        
    // Complete override
    assert(Seq(TSEntry(21, "Hy", 10))
        == tri.prepend(TSEntry(21, "Hy", 10)).entries)
        
    assert(Seq(TSEntry(22, "Hy", 10))
        == tri.prepend(TSEntry(22, "Hy", 10)).entries)
  }
  
  
  def testTs(startsAt: Long) = TreeMapTimeSeries(
      startsAt -> ("Ai", 10L), 
      startsAt + 10 -> ("Ao", 10L),
      startsAt + 20 -> ("Au", 10L)
      )
      
  @Test def appendTs() {
    // Append a multi-entry TS at various times on the entry  
    
    val tri = 
       TreeMapTimeSeries(
              1L -> ("Hi", 10L), 
              11L -> ("Ho", 10L),
              21L -> ("Hu", 10L))
              
    // Append after all entries
    assert(tri.entries ++ testTs(31).entries == tri.append(testTs(31)).entries)
    assert(tri.entries ++ testTs(32).entries == tri.append(testTs(32)).entries)
    
    // On last
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 9)) ++ testTs(30).entries 
        == tri.append(testTs(30)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 1)) ++ testTs(22).entries 
        == tri.append(testTs(22)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) ++ testTs(21).entries 
        == tri.append(testTs(21)).entries)
        
    // On second
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9)) ++ testTs(20).entries 
        == tri.append(testTs(20)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1)) ++ testTs(12).entries 
        == tri.append(testTs(12)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 10)) ++ testTs(11).entries 
        == tri.append(testTs(11)).entries)
        
    // On first
    assert(Seq(TSEntry(1, "Hi", 9)) ++ testTs(10).entries 
        == tri.append(testTs(10)).entries)
        
    assert(Seq(TSEntry(1, "Hi", 1)) ++ testTs(2).entries 
        == tri.append(testTs(2)).entries)
        
    assert(testTs(1).entries == tri.append(testTs(1)).entries)
    assert(testTs(0).entries == tri.append(testTs(0)).entries)
  }
  
  @Test def prependTs() {
    // Prepend a multi-entry TS at various times on the entry
    val tri = 
       TreeMapTimeSeries(
              1L -> ("Hi", 10L), 
              11L -> ("Ho", 10L),
              21L -> ("Hu", 10L))
    
    // Before all entries
    assert(testTs(-30).entries ++ tri.entries == tri.prepend(testTs(-30)).entries)
    assert(testTs(-29).entries ++ tri.entries == tri.prepend(testTs(-29)).entries)
    
    // On first
    assert(testTs(-28).entries ++ Seq(TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-28)).entries)
    
    assert(testTs(-20).entries ++ Seq(TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-20)).entries)
    
    assert(testTs(-19).entries ++ Seq(TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-19)).entries)
        
    // On second
    assert(testTs(-18).entries ++ Seq(TSEntry(12, "Ho", 9), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-18)).entries)
        
    assert(testTs(-10).entries ++ Seq(TSEntry(20, "Ho", 1), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-10)).entries)
        
    assert(testTs(-9).entries ++ Seq(TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-9)).entries)
    
    // On third  
    assert(testTs(-8).entries ++ Seq(TSEntry(22, "Hu", 9))
        == tri.prepend(testTs(-8)).entries)
    
    assert(testTs(0).entries ++ Seq(TSEntry(30, "Hu", 1))
        == tri.prepend(testTs(0)).entries)
        
    assert(testTs(1).entries == tri.prepend(testTs(1)).entries)
    assert(testTs(2).entries == tri.prepend(testTs(2)).entries)
    
  }
}