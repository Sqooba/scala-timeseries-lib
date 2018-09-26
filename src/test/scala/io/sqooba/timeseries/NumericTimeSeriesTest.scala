package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{TSEntry, VectorTimeSeries}
import org.junit.Test
import org.scalatest.junit.JUnitSuite

/**
  * Assumes the merge logic to be well tested:
  * just check that the general numerical operations
  * are implemented correctly.
  */
class NumericTimeSeriesTest extends JUnitSuite {

  val tsa = VectorTimeSeries(
    1L -> (1.0, 10L),
    // Leave a gap of 1 between the two entries
    12L -> (2.0, 10L))

  val tsb = VectorTimeSeries(
    6L -> (3.0, 10L))

  /**
    * Check that we only have a correct sum wherever
    * both time series are defined at the same time.
    */
  def testStrictPlus {

    assert(tsa + tsb == tsb + tsa)

    assert(Seq(TSEntry(6, 4.0, 5), TSEntry(12, 5.0, 4))
      == (tsa + tsb).entries)
  }

  /**
    * Check that we only have a correct subtraction wherever
    * both time series are defined at the same time.
    */
  @Test def testStrictMinus {

    assert(Seq(TSEntry(6, -2.0, 5), TSEntry(12, -1.0, 4))
      == tsa.minus(tsb).entries)

    assert(Seq(TSEntry(6, 2.0, 5), TSEntry(12, 1.0, 4))
      == tsb.minus(tsa).entries)
  }

  /**
    * Check that we only have a correct multiplication wherever
    * both time series are defined at the same time.
    */
  @Test def testStrictMultiply {

    assert(tsa * tsb == tsb * tsa)

    assert(Seq(TSEntry(6, 3.0, 5), TSEntry(12, 6.0, 4))
      == (tsb * tsa).entries)
  }

  @Test def testStepIntegral(): Unit = {
    // Easy cases...
    assert(
      NumericTimeSeries.stepIntegral[Int](Seq()) == Seq())
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(1, 2, 3000))) == Seq(TSEntry(1, 6.0, 3000)))

    // Sum the stuff!
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 10000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 30.0, 10000)))

    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 10000), TSEntry(20000, 3, 10000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 30.0, 10000), TSEntry(20000, 60.0, 10000)))

    // With some negative values for fun
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 0, 10000), TSEntry(20000, -1, 10000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 10.0, 10000), TSEntry(20000, 0.0, 10000)))

    // With different validities
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 1000), TSEntry(1000, 2, 10000)))
        == Seq(TSEntry(0, 1.0, 1000), TSEntry(1000, 21.0, 10000))
    )
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 1000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 12.0, 1000))
    )
  }

  @Test def testRollingFunctions(): Unit = {
    // using min as an aggregation function for the window
    val min = (in: Seq[Int]) => in.min

    assert(
      NumericTimeSeries.rolling(
        VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10))),
        min,
        10)
        == VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10)))
    )

    assert(
      NumericTimeSeries.rolling(
        VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10), TSEntry(30, 4, 10))),
        min,
        20)
        == VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10), TSEntry(20, 1, 10), TSEntry(30, 2, 10)))
    )

    assert(
      NumericTimeSeries.rolling(
        VectorTimeSeries.ofEntriesUnsafe(Seq()),
        min,
        20)
        == VectorTimeSeries.ofEntriesUnsafe(Seq())
    )

    // other tests with sum
    val sum = (in: Seq[Int]) => in.sum

    assert(
      NumericTimeSeries.rolling(
        VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10), TSEntry(30, 4, 10))),
        sum,
        20)
        == VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 3, 10), TSEntry(20, 6, 10), TSEntry(30, 9, 10)))
    )
  }

}