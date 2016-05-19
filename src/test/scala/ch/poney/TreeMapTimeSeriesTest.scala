package ch.poney

import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import org.junit.Test
import ch.poney.immutable.TreeMapTimeSeries
import scala.collection.immutable.TreeMap
import ch.poney.immutable.TreeMapTimeSeries
import ch.poney.immutable.TSValue
import ch.poney.immutable.EmptyTimeSeries

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
    assert(single.at(10).isDefined)
    assert(!single.at(11).isDefined)
    
    // With two entries
    val double = 
      new TreeMapTimeSeries(
          TreeMap(
              0L -> TSValue("Hi", 9), 
              10L -> TSValue("Ho", 9)))
    assert(2 == double.size)
    assert(!double.at(-1).isDefined)
    assert(double.at(0).get == "Hi")
    assert(double.at(9).get == "Hi")
    assert(double.at(10).get == "Ho")
    assert(double.at(19).get == "Ho")
    assert(!double.at(20).isDefined)
  }
  
  @Test def testSplit() {
    // With two entries
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
}