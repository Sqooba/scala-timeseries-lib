package ch.shastick

import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import org.junit.Test
import ch.shastick.immutable.TreeMapTimeSeries
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
              0L -> TSValue("Hi", 10), 
              20L -> TSValue("Ho", 10))

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
        1L -> TSValue("Hi", 10))
        
    assert(single.trimLeft(1) == single)
    assert(single.trimLeft(2) ==  
      TreeMapTimeSeries( 2L -> TSValue("Hi", 9)))
      
    assert(single.trimLeft(10) ==  
      TreeMapTimeSeries( 10L -> TSValue("Hi", 1)))
      
    assert(single.trimLeft(11) ==  EmptyTimeSeries())
    assert(single.trimLeft(12) ==  EmptyTimeSeries())
    
    // With two entries
    val double = 
      TreeMapTimeSeries(
              1L -> TSValue("Hi", 10), 
              11L -> TSValue("Ho", 10))
              
    assert(double.trimLeft(1) == double)
    assert(double.trimLeft(2) ==  
      TreeMapTimeSeries(
          2L -> TSValue("Hi", 9),
          11L -> TSValue("Ho", 10)))
      
    assert(double.trimLeft(10) ==  
      TreeMapTimeSeries( 
          10L -> TSValue("Hi", 1),
          11L -> TSValue("Ho", 10)))
      
    assert(double.trimLeft(11) ==  
      TreeMapTimeSeries( 
          11L -> TSValue("Ho", 10)))
          
    assert(double.trimLeft(12) ==  
      TreeMapTimeSeries( 
          12L -> TSValue("Ho", 9)))
          
    assert(double.trimLeft(20) ==  
      TreeMapTimeSeries( 
          20L -> TSValue("Ho", 1)))
          
   assert(double.trimLeft(21) ==  EmptyTimeSeries())
   assert(double.trimLeft(22) ==  EmptyTimeSeries())
  }
  
  @Test def testTrimRight() {
        // Check empty case...
    assert(TreeMapTimeSeries().trimRight(0) == EmptyTimeSeries())
    
    // With one entry
    val single = 
      TreeMapTimeSeries(
        1L -> TSValue("Hi", 10))
        
    assert(single.trimRight(11) == single)
    assert(single.trimRight(10) ==  
      TreeMapTimeSeries( 1L -> TSValue("Hi", 9)))
      
    assert(single.trimRight(2) ==  
      TreeMapTimeSeries( 1L -> TSValue("Hi", 1)))
      
    assert(single.trimRight(1) ==  EmptyTimeSeries())
    assert(single.trimRight(0) ==  EmptyTimeSeries())
    
    // With two entries
    val double = 
      TreeMapTimeSeries(
              1L -> TSValue("Hi", 10), 
              11L -> TSValue("Ho", 10))
              
    assert(double.trimRight(21) == double)
    assert(double.trimRight(20) ==  
      TreeMapTimeSeries(
          1L -> TSValue("Hi", 10),
          11L -> TSValue("Ho", 9)))
      
    assert(double.trimRight(12) ==  
      TreeMapTimeSeries( 
          1L -> TSValue("Hi", 10),
          11L -> TSValue("Ho", 1)))
      
    assert(double.trimRight(11) ==  
      TreeMapTimeSeries( 
          1L -> TSValue("Hi", 10)))
          
    assert(double.trimRight(10) ==  
      TreeMapTimeSeries( 
          1L -> TSValue("Hi", 9)))
          
    assert(double.trimRight(2) ==  
      TreeMapTimeSeries( 
          1L -> TSValue("Hi", 1)))
          
   assert(double.trimRight(1) ==  EmptyTimeSeries())
   assert(double.trimRight(0) ==  EmptyTimeSeries())
  }
  
  @Test def testSplit() {
    val tri = 
       TreeMapTimeSeries(
              0L -> TSValue("Hi", 10), 
              10L -> TSValue("Ho", 10),
              20L -> TSValue("Hu", 10))
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
              0L -> TSValue("Hi", 10), 
              10L -> TSValue("Ho", 10),
              20L -> TSValue("Hu", 10))
              
    val up = tri.map(s => s.toUpperCase())
    assert(3 == up.size())
    assert(up.at(0) == Some("HI"))
    assert(up.at(10) == Some("HO"))
    assert(up.at(20) == Some("HU"))
  }
}