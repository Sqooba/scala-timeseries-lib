package io.sqooba.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.timeseries.NumericTimeSeries.slidingIntegral
import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.junit.Test
import org.scalatest.junit.JUnitSuite

/**
  * Assumes the merge logic to be well tested:
  * just check that the general numerical operations
  * are implemented correctly.
  */
class NumericTimeSeriesTest extends JUnitSuite {

  val tsa = VectorTimeSeries(1L -> (1.0, 10L),
                             // Leave a gap of 1 between the two entries
                             12L -> (2.0, 10L))

  val tsb = TSEntry(6L, 3.0, 10L)

  /**
    * Check that we only have a correct sum wherever
    * both time series are defined at the same time.
    */
  @Test def testStrictPlus {

    assert(tsa + tsb == tsb + tsa)

    assert(
      Seq(TSEntry(6, 4.0, 5), TSEntry(12, 5.0, 4))
        == (tsa + tsb).entries
    )
  }

  /**
    * Check that we the resulting ts is defined even when
    * one time series is not defined.
    */
  @Test def testNonStrictPlus {
    assert(
      Seq(TSEntry(1, 1.0, 5), TSEntry(6, 4.0, 5), TSEntry(11, 3.0, 1), TSEntry(12, 5.0, 4), TSEntry(16, 2.0, 6))
        == tsa.plus(tsb, false).entries
    )
  }

  /**
    * Check that we only have a correct subtraction wherever
    * both time series are defined at the same time.
    */
  @Test def testStrictMinus {

    assert(
      Seq(TSEntry(6, -2.0, 5), TSEntry(12, -1.0, 4))
        == tsa.minus(tsb).entries
    )

    assert(
      Seq(TSEntry(6, 2.0, 5), TSEntry(12, 1.0, 4))
        == tsb.minus(tsa).entries
    )
  }

  /**
    * Check that we only have a correct multiplication wherever
    * both time series are defined at the same time.
    */
  @Test def testStrictMultiply {

    assert(tsa * tsb == tsb * tsa)

    assert(
      Seq(TSEntry(6, 3.0, 5), TSEntry(12, 6.0, 4))
        == (tsb * tsa).entries
    )
  }

  @Test def testStepIntegral(): Unit = {
    // Easy cases...
    assert(NumericTimeSeries.stepIntegral[Int](Seq()) == Seq())
    assert(NumericTimeSeries.stepIntegral(Seq(TSEntry(1, 2, 3000))) == Seq(TSEntry(1, 6.0, 3000)))

    // Sum the stuff!
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 10000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 30.0, 10000))
    )

    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 10000), TSEntry(20000, 3, 10000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 30.0, 10000), TSEntry(20000, 60.0, 10000))
    )

    // With some negative values for fun
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 0, 10000), TSEntry(20000, -1, 10000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 10.0, 10000), TSEntry(20000, 0.0, 10000))
    )

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
        10,
        false
      )
        == VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10)))
    )

    assert(
      NumericTimeSeries.rolling(
        VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10), TSEntry(30, 4, 10))),
        min,
        20,
        false
      )
        == VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10), TSEntry(20, 1, 10), TSEntry(30, 2, 10)))
    )

    assert(
      NumericTimeSeries.rolling(EmptyTimeSeries, min, 20)
        == EmptyTimeSeries
    )

    // other tests with sum
    val sum = (in: Seq[Int]) => in.sum

    assert(
      NumericTimeSeries.rolling(VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10), TSEntry(30, 4, 10))), sum, 20)
        == VectorTimeSeries.ofEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 3, 10), TSEntry(20, 6, 10), TSEntry(30, 9, 10)))
    )
  }

  @Test def testUnitWindowSliding: Unit = {
    // Mathematically this does not make much sense,
    // but here we essentially test that the behavior is as expected
    // This whole thing will be reworked at some point

    // Starting at 0
    val twoA = Seq(
      TSEntry(0, 1, 1),
      TSEntry(1, 2, 1)
    )

    assert(
      slidingIntegral(twoA, 1, TimeUnit.SECONDS) == twoA
    )

    // Starting at > 0
    val twoB = Seq(
      TSEntry(10, 1, 1),
      TSEntry(11, 2, 1)
    )

    assert(
      slidingIntegral(twoB, 1, TimeUnit.SECONDS) == twoB
    )

    // With durations > 1
    val twoC = Seq(
      TSEntry(10, 1, 2),
      TSEntry(12, 2, 2)
    )

    assert(
      slidingIntegral(twoC, 1, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 2.0, 2),
          TSEntry(12, 4.0, 2)
        )
    )

    // With a gap
    val twoD = Seq(
      TSEntry(10, 1, 1),
      TSEntry(12, 2, 1)
    )

    assert(
      slidingIntegral(twoD, 1, TimeUnit.SECONDS) == twoD
    )

    // With a gap and duration > 1
    val twoE = Seq(
      TSEntry(10, 1, 2),
      TSEntry(13, 2, 2)
    )

    assert(
      slidingIntegral(twoE, 1, TimeUnit.SECONDS) == Seq(
        TSEntry(10, 2, 2),
        TSEntry(13, 4, 2)
      )
    )

    // Three entries, of duration 1, two of then contiguous
    val tri = Seq(
      TSEntry(10, 1, 1),
      TSEntry(11, 2, 1),
      TSEntry(13, 2, 1)
    )

    assert(
      slidingIntegral(tri, 1, TimeUnit.SECONDS) == tri
    )
  }

  @Test def testSimpleSlidingSumContinuous: Unit = {
    // Test pair-wise continuous entries

    // Simple corner case
    assert(NumericTimeSeries.slidingIntegral[Int](Seq(), 10).isEmpty)

    // Case with one entry
    val single = Seq(TSEntry(10, 1, 10))

    assert(
      slidingIntegral(single, 1, TimeUnit.SECONDS) == Seq(TSEntry(10, 10, 10))
    )
    assert(
      slidingIntegral(single, 10, TimeUnit.SECONDS) == Seq(TSEntry(10, 10, 10))
    )
    assert(
      slidingIntegral(single, 20, TimeUnit.SECONDS) == Seq(TSEntry(10, 10, 10))
    )

    // Case with two contiguous entries
    val twoA = Seq(
      TSEntry(10, 1, 5),
      TSEntry(15, 2, 10)
    )

    // Window shorter than the shortest entry
    assert(
      slidingIntegral(twoA, 2, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 5),
          TSEntry(15, 25, 1),
          TSEntry(16, 20, 9)
        )
    )
    assert(
      slidingIntegral(twoA, 4, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 5),
          TSEntry(15, 25, 3),
          TSEntry(18, 20, 7)
        )
    )

    // Window equal to the shortest entry
    assert(
      slidingIntegral(twoA, 5, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 5),
          TSEntry(15, 25, 4),
          TSEntry(19, 20, 6)
        )
    )

    // Window equal to the longest entry
    assert(
      slidingIntegral(twoA, 10, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 5),
          TSEntry(15, 25, 9),
          TSEntry(24, 20, 1)
        )
    )

    // Window longer than the longest entry
    assert(
      slidingIntegral(twoA, 11, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 5),
          TSEntry(15, 25, 10)
        )
    )

    assert(
      slidingIntegral(twoA, 12, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 5),
          TSEntry(15, 25, 10)
        )
    )

    // Case with two contiguous entries
    val twoB = Seq(
      TSEntry(10, 1, 10),
      TSEntry(20, 2, 5)
    )

    // Window shorter than the shortest entry
    assert(
      slidingIntegral(twoB, 2, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 20, 1),
          TSEntry(21, 10, 4)
        )
    )
    assert(
      slidingIntegral(twoB, 4, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 20, 3),
          TSEntry(23, 10, 2)
        )
    )

    // Window equal to the shortest entry
    assert(
      slidingIntegral(twoB, 5, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 20, 4),
          TSEntry(24, 10, 1)
        )
    )

    // Window equal to the longest entry
    assert(
      slidingIntegral(twoB, 10, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 20, 5)
        )
    )

    // Window longer than the longest entry
    assert(
      slidingIntegral(twoB, 11, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 20, 5)
        )
    )

    assert(
      slidingIntegral(twoB, 12, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 20, 5)
        )
    )

  }

  @Test def testTripleContinuousEntriesSum: Unit = {
    // Test triple continuous entries with various configurations

    val triA =
      Seq(
        TSEntry(10, 1, 10),
        TSEntry(20, 2, 2),
        TSEntry(22, 3, 10)
      )

    assert(
      slidingIntegral(triA, 2, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 1),
          TSEntry(21, 4, 1),
          TSEntry(22, 34, 1),
          TSEntry(23, 30, 9)
        )
    )

    assert(
      slidingIntegral(triA, 3, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 2),
          TSEntry(22, 34, 2),
          TSEntry(24, 30, 8)
        )
    )

    assert(
      slidingIntegral(triA, 4, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 2),
          TSEntry(22, 44, 1),
          TSEntry(23, 34, 2),
          TSEntry(25, 30, 7)
        )
    )

    assert(
      slidingIntegral(triA, 10, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 2),
          TSEntry(22, 44, 7),
          TSEntry(29, 34, 2),
          TSEntry(31, 30, 1)
        )
    )

    assert(
      slidingIntegral(triA, 11, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 2),
          TSEntry(22, 44, 8),
          TSEntry(30, 34, 2)
        )
    )

    assert(
      slidingIntegral(triA, 12, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 2),
          TSEntry(22, 44, 9),
          TSEntry(31, 34, 1)
        )
    )

    assert(
      slidingIntegral(triA, 13, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 2),
          TSEntry(22, 44, 10)
        )
    )

    assert(
      slidingIntegral(triA, 14, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 10),
          TSEntry(20, 14, 2),
          TSEntry(22, 44, 10)
        )
    )
  }

  @Test def testSimpleSlidingSumDisContinuous: Unit = {
    // Test pair-wise discontinuous entries
    val twoA = Seq(
      TSEntry(10, 1, 5),
      TSEntry(17, 2, 10)
    )

    assert(
      slidingIntegral(twoA, 2, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 6),
          TSEntry(17, 20, 10)
        )
    )

    assert(
      slidingIntegral(twoA, 3, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 7),
          TSEntry(17, 20, 10)
        )
    )

    assert(
      slidingIntegral(twoA, 4, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 7),
          TSEntry(17, 25, 1),
          TSEntry(18, 20, 9)
        )
    )

    assert(
      slidingIntegral(twoA, 12, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 7),
          TSEntry(17, 25, 9),
          TSEntry(26, 20, 1)
        )
    )

    assert(
      slidingIntegral(twoA, 13, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 5, 7),
          TSEntry(17, 25, 10)
        )
    )

  }

  @Test def testTripleDiscontinuousEntriesSum: Unit = {
    // Test triple continuous entries with various configurations

    val triA =
      Seq(
        TSEntry(10, 1, 10),
        TSEntry(21, 2, 2),
        TSEntry(24, 3, 10)
      )

    assert(
      slidingIntegral(triA, 2, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 4, 3),
          TSEntry(24, 30, 10)
        )
    )

    assert(
      slidingIntegral(triA, 3, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 1),
          TSEntry(22, 4, 2),
          TSEntry(24, 34, 1),
          TSEntry(25, 30, 9)
        )
    )

    assert(
      slidingIntegral(triA, 4, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 2),
          TSEntry(23, 4, 1),
          TSEntry(24, 34, 2),
          TSEntry(26, 30, 8)
        )
    )

    assert(
      slidingIntegral(triA, 5, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 34, 3),
          TSEntry(27, 30, 7)
        )
    )

    assert(
      slidingIntegral(triA, 6, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 1),
          TSEntry(25, 34, 3),
          TSEntry(28, 30, 6)
        )
    )

    assert(
      slidingIntegral(triA, 7, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 2),
          TSEntry(26, 34, 3),
          TSEntry(29, 30, 5)
        )
    )

    assert(
      slidingIntegral(triA, 8, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 3),
          TSEntry(27, 34, 3),
          TSEntry(30, 30, 4)
        )
    )

    assert(
      slidingIntegral(triA, 10, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 5),
          TSEntry(29, 34, 3),
          TSEntry(32, 30, 2)
        )
    )

    assert(
      slidingIntegral(triA, 11, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 6),
          TSEntry(30, 34, 3),
          TSEntry(33, 30, 1)
        )
    )

    assert(
      slidingIntegral(triA, 12, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 7),
          TSEntry(31, 34, 3)
        )
    )

    assert(
      slidingIntegral(triA, 14, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 9),
          TSEntry(33, 34, 1)
        )
    )

    assert(
      slidingIntegral(triA, 15, TimeUnit.SECONDS) ==
        Seq(
          TSEntry(10, 10, 11),
          TSEntry(21, 14, 3),
          TSEntry(24, 44, 10)
        )
    )
  }

}
