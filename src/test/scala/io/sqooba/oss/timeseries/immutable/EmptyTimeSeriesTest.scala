package io.sqooba.oss.timeseries.immutable

import org.junit.Test
import org.scalatest.junit.JUnitSuite

class EmptyTimeSeriesTest extends JUnitSuite {

  private val ts           = EmptyTimeSeries
  private val testE        = TSEntry(1, "Test", 10)
  @Test def testAt(): Unit = assert(ts.at(1).isEmpty)

  @Test def testSize(): Unit = assert(ts.size == 0)

  @Test def testEmptiness: Unit = assert(ts.isEmpty)

  @Test def testNotCompressed: Unit = assert(!ts.isCompressed)

  @Test def testNotContinuous(): Unit = assert(!ts.isDomainContinuous)

  @Test def testDefined(): Unit = assert(!ts.defined(1))

  @Test def testTrimLeft(): Unit = assert(ts.trimLeft(-1) == EmptyTimeSeries)

  @Test def testTrimLeftDiscreteInclude: Unit = assert(ts.trimLeftDiscrete(1, true) == EmptyTimeSeries)

  @Test def testTrimLeftDiscreteExclude: Unit = assert(ts.trimLeftDiscrete(1, false) == EmptyTimeSeries)

  @Test def testTrimRightAt(): Unit = assert(ts.trimRight(1).isEmpty)

  @Test def testTrimRightDiscreteInclude: Unit = assert(ts.trimRightDiscrete(1, true) == EmptyTimeSeries)

  @Test def testTrimRightDiscreteExclude: Unit = assert(ts.trimRightDiscrete(1, false) == EmptyTimeSeries)

  @Test def testMap: Unit = assert(ts.map(n => None) == EmptyTimeSeries)

  @Test def testMapWithTime: Unit = assert(ts.mapWithTime((t, v) => t + v) == EmptyTimeSeries)

  @Test def testFilter: Unit = assert(ts.filter(_ => true) == EmptyTimeSeries)

  @Test def testFilterValues: Unit = assert(ts.filterValues(_ => true) == EmptyTimeSeries)

  @Test def testFill: Unit = assert(ts.fill("None") == EmptyTimeSeries)

  @Test def testEntries: Unit = assert(ts.entries == Seq())

  @Test def testHead: Unit = {
    intercept[NoSuchElementException] {
      ts.head
    }
  }

  @Test def testHeadOption: Unit = assert(ts.headOption.isEmpty)

  @Test def testHeadValue: Unit = {
    intercept[NoSuchElementException] {
      ts.headValue
    }
  }

  @Test def testHeadValueOption: Unit = assert(ts.headValueOption.isEmpty)

  @Test def testLast: Unit = {
    intercept[NoSuchElementException] {
      ts.last
    }
  }

  @Test def testLastOption: Unit = assert(ts.lastOption.isEmpty)

  @Test def testLastValue: Unit = {
    intercept[NoSuchElementException] {
      ts.lastValue
    }
  }

  @Test def testLastValueOption: Unit = assert(ts.lastValueOption.isEmpty)

  @Test def testAppend: Unit = assert(ts.append(testE) == testE)

  @Test def testPrepend: Unit = assert(ts.prepend(testE) == testE)

  @Test def testSlice: Unit = assert(ts.slice(-1, 1) == EmptyTimeSeries)

  @Test def testSplit: Unit = assert(ts.split(1) == (EmptyTimeSeries, EmptyTimeSeries))

}
