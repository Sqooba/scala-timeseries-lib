package io.sqooba.oss.timeseries

import java.util.concurrent.TimeUnit

import NumericTimeSeries.slidingIntegral
import io.sqooba.oss.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Assumes the merge logic to be well tested:
  * just check that the general numerical operations
  * are implemented correctly.
  */
class NumericTimeSeriesSpec extends FlatSpec with Matchers {

  private val tsa = TimeSeries(
    Seq(TSEntry(1, 1.0, 10), TSEntry(12, 2.0, 10))
  )

  private val tsb = TSEntry(6L, 3.0, 10L)

  "NumericTimeSeries" should "strictly add two time series" in {

    tsa + tsb shouldBe tsb + tsa

    (tsa + tsb).entries shouldBe
      Seq(TSEntry(6, 4.0, 5), TSEntry(12, 5.0, 4))
  }

  it should "non strictly add two time series" in {
    tsa.plus(tsb, false).entries shouldBe
      Seq(TSEntry(1, 1.0, 5), TSEntry(6, 4.0, 5), TSEntry(11, 3.0, 1), TSEntry(12, 5.0, 4), TSEntry(16, 2.0, 6))
  }

  it should "strictly subtract a time series from another" in {

    tsa.minus(tsb).entries shouldBe
      Seq(TSEntry(6, -2.0, 5), TSEntry(12, -1.0, 4))

    tsb.minus(tsa).entries shouldBe
      Seq(TSEntry(6, 2.0, 5), TSEntry(12, 1.0, 4))
  }

  it should "still strictly subtract a time series from another if both defaults are None" in {

    tsa.minus(tsb) shouldBe tsa.merge(NumericTimeSeries.nonStrictMinus(None, None))(tsb)
  }

  it should "non strictly subtract a time series from another if the left default is given" in {
    tsa.merge[Double, Double](NumericTimeSeries.nonStrictMinus(Some(0), None))(tsb).entries shouldBe
      Seq(TSEntry(6L, -2.0, 5), TSEntry(11, -3.0, 1), TSEntry(12L, -1, 4))
  }

  it should "non strictly subtract a time series from another if the right default is given" in {
    tsa.merge[Double, Double](NumericTimeSeries.nonStrictMinus(None, Some(10)))(tsb).entries shouldBe
      Seq(TSEntry(1L, -9, 5), TSEntry(6L, -2.0, 5), TSEntry(12L, -1, 4), TSEntry(16L, -8, 6))
  }

  it should "non strictly subtract a time series from another if both defaults are given" in {
    tsa.merge[Double, Double](NumericTimeSeries.nonStrictMinus(Some(0.5), Some(10)))(tsb).entries shouldBe
      Seq(TSEntry(1L, -9, 5), TSEntry(6L, -2.0, 5), TSEntry(11, -2.5, 1), TSEntry(12L, -1, 4), TSEntry(16L, -8, 6))
  }

  it should "strictly multiply two time series" in {
    tsa * tsb shouldBe tsb * tsa

    (tsb * tsa).entries shouldBe
      Seq(TSEntry(6, 3.0, 5), TSEntry(12, 6.0, 4))
  }

  it should "correctly do a step integral" in {
    // Easy cases...
    assert(NumericTimeSeries.stepIntegral[Int](Seq()) == Seq())
    assert(NumericTimeSeries.stepIntegral(Seq(TSEntry(1, 2, 3000))) == Seq(TSEntry(1, 6.0, 3000)))

    // Sum the stuff!
    assert(
      NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 10000)))
        == Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 30.0, 10000))
    )

    NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 10000), TSEntry(20000, 3, 10000))) shouldBe
      Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 30.0, 10000), TSEntry(20000, 60.0, 10000))

    // With some negative values for fun
    NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 0, 10000), TSEntry(20000, -1, 10000))) shouldBe
      Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 10.0, 10000), TSEntry(20000, 0.0, 10000))

    // With different validities
    NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 1000), TSEntry(1000, 2, 10000))) shouldBe
      Seq(TSEntry(0, 1.0, 1000), TSEntry(1000, 21.0, 10000))
    NumericTimeSeries.stepIntegral(Seq(TSEntry(0, 1, 10000), TSEntry(10000, 2, 1000))) shouldBe
      Seq(TSEntry(0, 10.0, 10000), TSEntry(10000, 12.0, 1000))
  }

  it should "do an aggregation with rolling" in {
    // using min as an aggregation function for the window
    val min = (in: Seq[Int]) => in.min

    val minNotCompressedSeries1 = NumericTimeSeries.rolling(
      TimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10))),
      min,
      10,
      false
    )

    minNotCompressedSeries1.entries shouldBe
      Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10))
    assert(!minNotCompressedSeries1.isCompressed)

    val minNotCompressedSeries2 = NumericTimeSeries.rolling(
      TimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10), TSEntry(30, 4, 10))),
      min,
      20,
      false
    )

    minNotCompressedSeries2.entries shouldBe
      Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10), TSEntry(20, 1, 10), TSEntry(30, 2, 10))

    assert(!minNotCompressedSeries2.isCompressed)

    NumericTimeSeries.rolling(EmptyTimeSeries, min, 20) shouldBe
      EmptyTimeSeries

    // other tests with sum
    val sum = (in: Seq[Int]) => in.sum

    val sumCompressSeries = NumericTimeSeries.rolling(
      TimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10), TSEntry(30, 4, 10))),
      sum,
      20
    )

    sumCompressSeries.entries shouldBe
      Seq(TSEntry(0, 1, 10), TSEntry(10, 3, 10), TSEntry(20, 6, 10), TSEntry(30, 9, 10))

    assert(sumCompressSeries.isCompressed)
  }

  private val single = Seq(TSEntry(10, 1, 10))

  it should "not allow a window smaller than the sample rate in the slidingIntegral" in {
    an[IllegalArgumentException] should be thrownBy slidingIntegral(single, 1, 10, TimeUnit.SECONDS)
  }

  it should "slidingly integrate a series of pair-wise contiguous entries" in {
    // Simple corner case
    slidingIntegral[Int](Seq(), 10, 10) shouldBe Seq()

    slidingIntegral(single, 10, 10, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 10, 10))

    slidingIntegral(single, 20, 10, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 10, 10))

    // Case with two contiguous entries
    val twoA = Seq(
      TSEntry(10, 1, 5),
      TSEntry(15, 2, 10)
    )

    // Window shorter than the shortest entry
    slidingIntegral(twoA, 3, 3, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 3.0, 3), TSEntry(13, 6.0, 3), TSEntry(16, 9.0, 3), TSEntry(19, 12.0, 3), TSEntry(22, 12.0, 3))

    // Window equal to the shortest entry
    slidingIntegral(twoA, 5, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 15, 5), TSEntry(20, 20, 5))

    // Window equal to the longest entry
    slidingIntegral(twoA, 10, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 15, 5), TSEntry(20, 25, 5))

    // Window longer than the longest entry
    slidingIntegral(twoA, 12, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 15, 5), TSEntry(20, 25, 5))

    // Case with two contiguous entries
    val twoB = Seq(
      TSEntry(10, 1, 10),
      TSEntry(20, 2, 5)
    )

    // Window shorter than the shortest entry
    slidingIntegral(twoB, 4, 2, TimeUnit.SECONDS) shouldBe
      Seq(
        TSEntry(10, 2, 2),
        TSEntry(12, 4, 2),
        TSEntry(14, 6, 2),
        TSEntry(16, 6, 2),
        TSEntry(18, 6, 2),
        TSEntry(20, 8, 2),
        TSEntry(22, 10, 2),
        TSEntry(24, 12, 2)
      )

    // Window equal to the shortest entry
    slidingIntegral(twoB, 5, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 10, 5), TSEntry(20, 15, 5))

    slidingIntegral(twoB, 9, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 10, 5), TSEntry(20, 20.0, 4), TSEntry(24, 15.0, 1))

    // Window equal to the longest entry
    slidingIntegral(twoB, 10, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 10, 5), TSEntry(20, 20.0, 5))

    // Window longer than the longest entry
    slidingIntegral(twoB, 15, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 10, 5), TSEntry(20, 20.0, 5))
  }

  it should "slidingly integrate a series of triple contiguous entries with various configurations" in {
    val triA =
      Seq(
        TSEntry(10, 1, 10),
        TSEntry(20, 2, 2),
        TSEntry(22, 3, 10)
      )

    slidingIntegral(triA, 2, 2, TimeUnit.SECONDS) shouldBe
      Seq(
        TSEntry(10, 2, 2),
        TSEntry(12, 4, 2),
        TSEntry(14, 4, 2),
        TSEntry(16, 4, 2),
        TSEntry(18, 4, 2),
        TSEntry(20, 6, 2),
        TSEntry(22, 10, 2),
        TSEntry(24, 12, 2),
        TSEntry(26, 12, 2),
        TSEntry(28, 12, 2),
        TSEntry(30, 12, 2)
      )

    slidingIntegral(triA, 4, 2, TimeUnit.SECONDS) shouldBe
      Seq(
        TSEntry(10, 2, 2),
        TSEntry(12, 4, 2),
        TSEntry(14, 6, 2),
        TSEntry(16, 6, 2),
        TSEntry(18, 6, 2),
        TSEntry(20, 8, 2),
        TSEntry(22, 12, 2),
        TSEntry(24, 16, 2),
        TSEntry(26, 18, 2),
        TSEntry(28, 18, 2),
        TSEntry(30, 18, 2)
      )

    slidingIntegral(triA, 9, 3, TimeUnit.SECONDS) shouldBe
      Seq(
        TSEntry(10, 3.0, 3),
        TSEntry(13, 6.0, 3),
        TSEntry(16, 9.0, 3),
        TSEntry(19, 15.0, 3),
        TSEntry(22, 21.0, 3),
        TSEntry(25, 27.0, 3),
        TSEntry(28, 33.0, 3),
        TSEntry(31, 36.0, 3)
      )

    slidingIntegral(triA, 12, 8, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 8.0, 8), TSEntry(18, 24.0, 8), TSEntry(26, 48.0, 4), TSEntry(30, 40.0, 4))
  }

  it should "slidingly integrate a series of non-contiguous entries" in {
    val twoA = Seq(
      TSEntry(10, 1, 5),
      TSEntry(17, 2, 10)
    )

    slidingIntegral(twoA, 2, 2, TimeUnit.SECONDS) shouldBe
      Seq(
        TSEntry(10, 2, 2),
        TSEntry(12, 4, 2),
        TSEntry(14, 4, 2),
        TSEntry(16, 6, 2),
        TSEntry(18, 8, 2),
        TSEntry(20, 8, 2),
        TSEntry(22, 8, 2),
        TSEntry(24, 8, 2),
        TSEntry(26, 8, 2)
      )

    slidingIntegral(twoA, 5, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5, 5), TSEntry(15, 15, 5), TSEntry(20, 20, 5), TSEntry(25, 20, 5))

    slidingIntegral(twoA, 12, 5, TimeUnit.SECONDS) shouldBe
      Seq(TSEntry(10, 5.0, 5), TSEntry(15, 15.0, 5), TSEntry(20, 25.0, 5), TSEntry(25, 35.0, 2), TSEntry(27, 30.0, 3))
  }
}
