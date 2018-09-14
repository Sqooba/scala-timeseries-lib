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
      NumericTimeSeries.stepIntegral(Seq(TSEntry(1, 2, 3))) == Seq(TSEntry(1, BigDecimal(6), 3)))

    // Sum the stuff!
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10)))
        == Seq(TSEntry(0, BigDecimal(10), 10), TSEntry(10, BigDecimal(30), 10)))

    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10)))
        == Seq(TSEntry(0, BigDecimal(10), 10), TSEntry(10, BigDecimal(30), 10), TSEntry(20, BigDecimal(60), 10)))

    // With some negative values for fun
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10), TSEntry(10, 0, 10), TSEntry(20, -1, 10)))
        == Seq(TSEntry(0, BigDecimal(10), 10), TSEntry(10, BigDecimal(10), 10), TSEntry(20, BigDecimal(0), 10)))

    // With different validities
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 1), TSEntry(1, 2, 10)))
        == Seq(TSEntry(0, BigDecimal(1), 1), TSEntry(1, BigDecimal(21), 10))
    )
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 1)))
        == Seq(TSEntry(0, BigDecimal(10), 10), TSEntry(10, BigDecimal(12), 1))
    )

  }


}