package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry}
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class EmptyTimeSeriesTest extends JUnitSuite {

  val ts = EmptyTimeSeries[String]()
  val testE = TSEntry(1, "Test", 10)

  @Test def testAt: Unit = assert(ts.at(1) == None)

  @Test def testSize: Unit = assert(ts.size == 0)

  @Test def testDefined: Unit = assert(ts.defined(1) == false)

  @Test def testTrimLeft: Unit = assert(ts.trimLeft(1) == EmptyTimeSeries())

  @Test def testTrimRight: Unit = assert(ts.trimRight(1) == EmptyTimeSeries())

  @Test def testMap: Unit = assert(ts.map(n => None) == EmptyTimeSeries())

  @Test def testMapWithTime(): Unit = assert(ts.mapWithTime( (t, v) => t + v ) == EmptyTimeSeries())

  @Test def testEntries: Unit = assert(ts.entries == Seq())

  @Test def testHead(): Unit = {
    intercept[NoSuchElementException] {
      ts.head
    }
  }

  @Test def testHeadOption: Unit = assert(ts.headOption == None)

  @Test def testLast(): Unit = {
    intercept[NoSuchElementException] {
      ts.last
    }
  }

  @Test def testLastOption: Unit = assert(ts.lastOption == None)

  @Test def testAppend: Unit = assert(ts.append(testE) == testE)

  @Test def testPrepend: Unit = assert(ts.prepend(testE) == testE)

  @Test def testSlice: Unit = assert(ts.slice(-1, 1) == EmptyTimeSeries())

  @Test def testSplit: Unit = assert(ts.split(1) == (EmptyTimeSeries(), EmptyTimeSeries()))

}