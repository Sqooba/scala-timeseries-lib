package ch.shastick

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import ch.shastick.immutable.EmptyTimeSeries
import ch.shastick.immutable.TSEntry

class EmptyTimeSeriesTest extends JUnitSuite {
  
  val ts = EmptyTimeSeries[String]()
  val testE = TSEntry(1, "Test", 10)
  
  @Test def testAt = assert(ts.at(1) == None)
  
  @Test def testSize = assert(ts.size == 0)
  
  @Test def testDefined = assert(ts.defined(1) == false)
  
  @Test def testTrimLeft = assert(ts.trimLeft(1) == EmptyTimeSeries())
  
  @Test def testTrimRight = assert(ts.trimRight(1) == EmptyTimeSeries())
  
  @Test def testMap = assert(ts.map(n => None) == EmptyTimeSeries())
  
  @Test def testEntries = assert(ts.entries == Seq())
  
  @Test def testHead() {
    intercept[NoSuchElementException] {
      ts.head
    }
  }
  
  @Test def testHeadOption = assert(ts.headOption == None)
  
  @Test def testLast() {
    intercept[NoSuchElementException] {
      ts.last
    }
  }
  
  @Test def testLastOption = assert(ts.lastOption == None)
  
  @Test def testAppend = assert(ts.append(testE) == testE)
  
  @Test def testPrepend = assert(ts.prepend(testE) == testE)
  
}