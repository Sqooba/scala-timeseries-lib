package ch.poney

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test
import ch.poney.immutable.TreeMapTimeSeries
import scala.collection.immutable.TreeMap
import ch.poney.immutable.TreeMapTimeSeries

class TreeMapTimeSeriesTest {
  
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
}