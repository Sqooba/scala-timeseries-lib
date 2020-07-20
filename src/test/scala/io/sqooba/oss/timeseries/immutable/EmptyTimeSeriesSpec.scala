package io.sqooba.oss.timeseries.immutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class EmptyTimeSeriesSpec extends AnyFlatSpec with should.Matchers {

  private val ts    = EmptyTimeSeries
  private val testE = TSEntry(1, "Test", 10)

  "EmptyTimeSeries" should "correctly do At" in {
    assert(ts.at(1).isEmpty)

  }
  it should "correctly do Size" in {
    assert(ts.size == 0)
  }
  it should "correctly do Emptiness" in {
    assert(ts.isEmpty)
  }
  it should "correctly do NotCompressed" in {
    assert(!ts.isCompressed)
  }
  it should "correctly do NotContinuous" in {
    assert(!ts.isDomainContinuous)
  }
  it should "correctly do Defined" in {
    assert(!ts.defined(1))
  }
  it should "correctly do TrimLeft" in {
    assert(ts.trimLeft(-1) == EmptyTimeSeries)
  }
  it should "correctly do TrimLeftDiscreteInclude" in {
    assert(ts.trimLeftDiscrete(1, true) == EmptyTimeSeries)
  }
  it should "correctly do TrimLeftDiscreteExclude" in {
    assert(ts.trimLeftDiscrete(1, false) == EmptyTimeSeries)
  }
  it should "correctly do TrimRightAt" in {
    assert(ts.trimRight(1).isEmpty)
  }
  it should "correctly do TrimRightDiscreteInclude" in {
    assert(ts.trimRightDiscrete(1, true) == EmptyTimeSeries)
  }
  it should "correctly do TrimRightDiscreteExclude" in {
    assert(ts.trimRightDiscrete(1, false) == EmptyTimeSeries)
  }
  it should "correctly do Map" in {
    assert(ts.map(n => None) == EmptyTimeSeries)
  }
  it should "correctly do MapWithTime" in {
    assert(ts.mapWithTime((t, v) => t + v) == EmptyTimeSeries)
  }
  it should "correctly do Filter" in {
    assert(ts.filter(_ => true) == EmptyTimeSeries)
  }
  it should "correctly do FilterValues" in {
    assert(ts.filterValues(_ => true) == EmptyTimeSeries)
  }
  it should "correctly do Fill" in {
    assert(ts.fill("None") == EmptyTimeSeries)
  }
  it should "correctly do Entries" in {
    assert(ts.entries == Seq())
  }
  it should "correctly do Values" in {
    assert(ts.values == Seq())
  }
  it should "correctly do Head" in {

    intercept[NoSuchElementException] {
      ts.head
    }

  }
  it should "correctly do HeadOption" in {

    assert(ts.headOption.isEmpty)
  }
  it should "correctly do HeadValue" in {

    intercept[NoSuchElementException] {
      ts.headValue
    }
  }

  it should "correctly do HeadValueOption" in {
    assert(ts.headValueOption.isEmpty)
  }
  it should "correctly do Last" in {

    intercept[NoSuchElementException] {
      ts.last
    }
  }

  it should "correctly do LastOption" in {
    assert(ts.lastOption.isEmpty)
  }
  it should "correctly do LastValue" in {

    intercept[NoSuchElementException] {
      ts.lastValue
    }
  }

  it should "correctly do LastValueOption" in {
    assert(ts.lastValueOption.isEmpty)
  }
  it should "correctly do Append" in {
    assert(ts.append(testE) == testE)
  }
  it should "correctly do Prepend" in {
    assert(ts.prepend(testE) == testE)
  }

  it should "correctly do Slice" in {
    assert(ts.slice(-1, 1) == EmptyTimeSeries)
  }

  it should "correctly do Split" in {
    assert(ts.split(1) == (EmptyTimeSeries, EmptyTimeSeries))
  }

  it should "correctly do Rollup" in {
    assert(ts.rollup(Stream.empty, _ => throw new IllegalStateException()) == EmptyTimeSeries)
  }
}
