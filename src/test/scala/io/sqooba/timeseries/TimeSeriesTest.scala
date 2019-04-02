package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TimeSeriesTest extends JUnitSuite {

  // Simple non-strict summing operator
  def plus(aO: Option[Double], bO: Option[Double]): Option[Double] = {
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a + b)
      case (Some(_), None)    => aO
      case (None, Some(_))    => bO
      case _                  => None
    }
  }

  def mul(aO: Option[Double], bO: Option[Double]): Option[Double] = {
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a.doubleValue * b.doubleValue)
      case (Some(x), None)    => Some(x)
      case (None, Some(x))    => Some(x)
      case _                  => None
    }
  }

  @Test def testSeqMergingSingleToMultiple(): Unit = {
    // Single to single within domain.
    val s1 = Seq(TSEntry(1, 2.0, 20))
    val m1 = Seq(TSEntry(5, 1.0, 10))

    assert(
      TimeSeries.mergeEntries(s1)(m1)(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6))
    )

    assert(
      TimeSeries.mergeEntries(m1)(s1)(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6))
    )

    // Merging with two entries wholly contained in the single's domain
    val s3 = Seq(TSEntry(1, 2.0, 20))
    val m3 = Seq(TSEntry(5, 1.0, 5), TSEntry(10, 2.0, 5))

    assert(
      TimeSeries.mergeEntries(s3)(m3)(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6))
    )

    assert(
      TimeSeries.mergeEntries(m3)(s3)(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6))
    )

    val s4 = Seq(TSEntry(1, 2.0, 20))
    val m4 = Seq(TSEntry(5, 1.0, 5), TSEntry(11, 2.0, 5))

    assert(
      TimeSeries.mergeEntries(s4)(m4)(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5))
    )

    assert(
      TimeSeries.mergeEntries(m4)(s4)(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5))
    )

    // Merge with three entries, the first and last one exceeding the single's domain
    val s5 = Seq(TSEntry(1, 2.0, 20))
    val m5 = Seq(TSEntry(0, 1.0, 5), TSEntry(5, 2.0, 5), TSEntry(16, 3.0, 10))

    assert(
      TimeSeries.mergeEntries(s5)(m5)(plus) ==
        Seq(TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5))
    )

    assert(
      TimeSeries.mergeEntries(m5)(s5)(plus) ==
        Seq(TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5))
    )

    // Merge with four entries, the first and last one being completely outside of the single's domain
    val s6 = Seq(TSEntry(1, 2.0, 20))
    val m6 = Seq(TSEntry(-10, -1.0, 10), TSEntry(0, 1.0, 5), TSEntry(6, 2.0, 5), TSEntry(16, 3.0, 10), TSEntry(26, 4.0, 10))

    assert(
      TimeSeries.mergeEntries(s6)(m6)(plus) ==
        Seq(
          TSEntry(-10, -1, 10),
          TSEntry(0, 1.0, 1),
          TSEntry(1, 3.0, 4),
          TSEntry(5, 2.0, 1),
          TSEntry(6, 4.0, 5),
          TSEntry(11, 2.0, 5),
          TSEntry(16, 5.0, 5),
          TSEntry(21, 3.0, 5),
          TSEntry(26, 4.0, 10)
        )
    )

    assert(
      TimeSeries.mergeEntries(s6)(m6)(plus) ==
        Seq(
          TSEntry(-10, -1, 10),
          TSEntry(0, 1.0, 1),
          TSEntry(1, 3.0, 4),
          TSEntry(5, 2.0, 1),
          TSEntry(6, 4.0, 5),
          TSEntry(11, 2.0, 5),
          TSEntry(16, 5.0, 5),
          TSEntry(21, 3.0, 5),
          TSEntry(26, 4.0, 10)
        )
    )

  }

  @Test def testContinuousDomainMerges(): Unit = {
    // Perfectly aligned, no discontinuities
    val l1 = Seq(TSEntry(-20, 1.0, 10), TSEntry(-10, 2.0, 10), TSEntry(0, 3.0, 10), TSEntry(10, 4.0, 10))
    val r1 = Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 6.0, 10), TSEntry(0, 7.0, 10), TSEntry(10, 8.0, 10))

    assert(
      TimeSeries.mergeEntries(l1)(r1)(mul) ==
        Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 12.0, 10), TSEntry(0, 21.0, 10), TSEntry(10, 32.0, 10))
    )

    assert(
      TimeSeries.mergeEntries(r1)(l1)(mul) ==
        Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 12.0, 10), TSEntry(0, 21.0, 10), TSEntry(10, 32.0, 10))
    )

    // Shifted
    val r2 = r1.map(e => TSEntry(e.timestamp + 5, e.value, e.validity))

    assert(
      TimeSeries.mergeEntries(l1)(r2)(mul) ==
        Seq(
          TSEntry(-20, 1.0, 5),
          TSEntry(-15, 5.0, 5),
          TSEntry(-10, 10.0, 5),
          TSEntry(-5, 12.0, 5),
          TSEntry(0, 18.0, 5),
          TSEntry(5, 21.0, 5),
          TSEntry(10, 28.0, 5),
          TSEntry(15, 32.0, 5),
          TSEntry(20, 8.0, 5)
        )
    )

    assert(
      TimeSeries.mergeEntries(r2)(l1)(mul) ==
        Seq(
          TSEntry(-20, 1.0, 5),
          TSEntry(-15, 5.0, 5),
          TSEntry(-10, 10.0, 5),
          TSEntry(-5, 12.0, 5),
          TSEntry(0, 18.0, 5),
          TSEntry(5, 21.0, 5),
          TSEntry(10, 28.0, 5),
          TSEntry(15, 32.0, 5),
          TSEntry(20, 8.0, 5)
        )
    )

    // Denser second sequence
    // Perfectly aligned
    val r3 = Seq(TSEntry(-20, 5.0, 5),
                 TSEntry(-15, 6.0, 5),
                 TSEntry(-10, 7.0, 5),
                 TSEntry(-5, 8.0, 5),
                 TSEntry(0, 9.0, 5),
                 TSEntry(5, 10.0, 5),
                 TSEntry(10, 11.0, 5),
                 TSEntry(15, 12.0, 5))

    assert(
      TimeSeries.mergeEntries(l1)(r3)(mul) ==
        Seq(
          TSEntry(-20, 5.0, 5),
          TSEntry(-15, 6.0, 5),
          TSEntry(-10, 14.0, 5),
          TSEntry(-5, 16.0, 5),
          TSEntry(0, 27.0, 5),
          TSEntry(5, 30.0, 5),
          TSEntry(10, 44.0, 5),
          TSEntry(15, 48.0, 5)
        )
    )

    assert(
      TimeSeries.mergeEntries(r3)(l1)(mul) ==
        Seq(
          TSEntry(-20, 5.0, 5),
          TSEntry(-15, 6.0, 5),
          TSEntry(-10, 14.0, 5),
          TSEntry(-5, 16.0, 5),
          TSEntry(0, 27.0, 5),
          TSEntry(5, 30.0, 5),
          TSEntry(10, 44.0, 5),
          TSEntry(15, 48.0, 5)
        )
    )

    // Shifted
    val r4 = r3.map(e => TSEntry(e.timestamp + 4, e.value, e.validity))

    assert(
      TimeSeries.mergeEntries(l1)(r4)(mul) ==
        Seq(
          TSEntry(-20, 1.0, 4),
          TSEntry(-16, 5.0, 5),
          TSEntry(-11, 6.0, 1),
          TSEntry(-10, 12.0, 4),
          TSEntry(-6, 14.0, 5),
          TSEntry(-1, 16.0, 1),
          TSEntry(0, 24.0, 4),
          TSEntry(4, 27.0, 5),
          TSEntry(9, 30.0, 1),
          TSEntry(10, 40.0, 4),
          TSEntry(14, 44.0, 5),
          TSEntry(19, 48.0, 1),
          TSEntry(20, 12.0, 4)
        )
    )

    assert(
      TimeSeries.mergeEntries(r4)(l1)(mul) ==
        Seq(
          TSEntry(-20, 1.0, 4),
          TSEntry(-16, 5.0, 5),
          TSEntry(-11, 6.0, 1),
          TSEntry(-10, 12.0, 4),
          TSEntry(-6, 14.0, 5),
          TSEntry(-1, 16.0, 1),
          TSEntry(0, 24.0, 4),
          TSEntry(4, 27.0, 5),
          TSEntry(9, 30.0, 1),
          TSEntry(10, 40.0, 4),
          TSEntry(14, 44.0, 5),
          TSEntry(19, 48.0, 1),
          TSEntry(20, 12.0, 4)
        )
    )

  }

  @Test def testSlice: Unit = {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))
    assert(tri.slice(-1, 0) == EmptyTimeSeries)
    assert(tri.slice(-1, 10).entries == Seq(TSEntry(0, "Hi", 10)))
    assert(tri.slice(0, 10).entries == Seq(TSEntry(0, "Hi", 10)))
    assert(tri.slice(0, 9).entries == Seq(TSEntry(0, "Hi", 9)))
    assert(tri.slice(1, 10).entries == Seq(TSEntry(1, "Hi", 9)))
    assert(tri.slice(9, 11).entries == Seq(TSEntry(9, "Hi", 1), TSEntry(10, "Ho", 1)))
    assert(tri.slice(9, 20).entries == Seq(TSEntry(9, "Hi", 1), TSEntry(10, "Ho", 10)))
    assert(tri.slice(10, 20).entries == Seq(TSEntry(10, "Ho", 10)))
    assert(tri.slice(15, 20).entries == Seq(TSEntry(15, "Ho", 5)))
    assert(tri.slice(15, 25).entries == Seq(TSEntry(15, "Ho", 5), TSEntry(20, "Hu", 5)))
    assert(tri.slice(20, 25).entries == Seq(TSEntry(20, "Hu", 5)))
    assert(tri.slice(25, 30).entries == Seq(TSEntry(25, "Hu", 5)))
    assert(tri.slice(25, 35).entries == Seq(TSEntry(25, "Hu", 5)))

  }

  @Test def testSliceDiscrete: Unit = {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    assert(tri == tri.sliceDiscrete(5, 25, true, true))
    assert(tri.slice(0, 20) == tri.sliceDiscrete(5, 25, true, false))
    assert(tri.slice(10, 30) == tri.sliceDiscrete(5, 25, false, true))
    assert(tri.slice(10, 20) == tri.sliceDiscrete(5, 25, false, false))

    assert(TSEntry(10, "Ho", 10) == tri.sliceDiscrete(10, 20, true, true))
    assert(TSEntry(10, "Ho", 10) == tri.sliceDiscrete(10, 20, false, false))
  }

  @Test def testSplitDiscrete: Unit = {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    assert(
      (VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L)), TSEntry(20, "Hu", 10))
        == tri.splitDiscrete(15, true)
    )

    assert(
      (TSEntry(0L, "Hi", 10L), VectorTimeSeries(10L -> ("Ho", 10L), 20L -> ("Hu", 10L)))
        == tri.splitDiscrete(15, false)
    )

    assert(
      (tri, EmptyTimeSeries)
        == tri.splitDiscrete(25, true)
    )

    assert(
      (VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L)), TSEntry(20, "Hu", 10))
        == tri.splitDiscrete(25, false)
    )

    assert(
      (TSEntry(0L, "Hi", 10L), VectorTimeSeries(10L -> ("Ho", 10L), 20L -> ("Hu", 10L)))
        == tri.splitDiscrete(5, true)
    )

    assert(
      (EmptyTimeSeries, tri) == tri.splitDiscrete(5, false)
    )

  }

  @Test def testFitTSEntries(): Unit = {
    // Test the simple cases
    assert(TimeSeries.fitAndCompressTSEntries(Seq()).isEmpty)
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(20, "B", 10)))
        == Seq(TSEntry(20, "B", 10))
    )

    // Test non-overlapping entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(10, "A", 10), TSEntry(20, "B", 10), TSEntry(30, "C", 10)))
        == Seq(TSEntry(10, "A", 10), TSEntry(20, "B", 10), TSEntry(30, "C", 10))
    )

    // Test non-overlapping entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(10, "A", 11), TSEntry(20, "B", 12), TSEntry(30, "C", 13)))
        == Seq(TSEntry(10, "A", 10), TSEntry(20, "B", 10), TSEntry(30, "C", 13))
    )
  }

  @Test def testFitVectors(): Unit = {
    // Vectors need their own unapply method: this
    // test makes sure it is used.

    // Test the simple case
    assert(TimeSeries.fitAndCompressTSEntries(Vector()).isEmpty)

    // Test non-overlapping entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(10, "A", 10), TSEntry(20, "B", 10), TSEntry(30, "C", 10)))
        == Seq(TSEntry(10, "A", 10), TSEntry(20, "B", 10), TSEntry(30, "C", 10))
    )

    // Test overlapping entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(10, "A", 11), TSEntry(20, "B", 12), TSEntry(30, "C", 13)))
        == Seq(TSEntry(10, "A", 10), TSEntry(20, "B", 10), TSEntry(30, "C", 13))
    )
  }

  @Test def testCompressTSEntries(): Unit = {

    // Simple cases
    assert(TimeSeries.fitAndCompressTSEntries(Seq()).isEmpty)
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(20, "B", 10)))
        == Seq(TSEntry(20, "B", 10))
    )

    // Test non contiguous entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(10, "A", 9), TSEntry(20, "A", 9), TSEntry(30, "A", 10)))
        == Seq(TSEntry(10, "A", 9), TSEntry(20, "A", 9), TSEntry(30, "A", 10))
    )

    // Test two contiguous and equal entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(10, "A", 10), TSEntry(20, "A", 10)))
        == Seq(TSEntry(10, "A", 20))
    )

    // Test three contiguous and equal entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(10, "A", 10), TSEntry(20, "A", 10), TSEntry(30, "A", 10)))
        == Seq(TSEntry(10, "A", 30))
    )

    // Test four contiguous entries, the two first being unequal
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(10, "B", 10), TSEntry(20, "A", 10), TSEntry(30, "A", 10), TSEntry(40, "A", 10)))
        == Seq(TSEntry(10, "B", 10), TSEntry(20, "A", 30))
    )

    // Test five contiguous entries, the third one being unequal to the others
    assert(
      TimeSeries.fitAndCompressTSEntries(Seq(TSEntry(10, "A", 10), TSEntry(20, "A", 10), TSEntry(30, "B", 10), TSEntry(40, "A", 10), TSEntry(50, "A", 10)))
        == Seq(TSEntry(10, "A", 20), TSEntry(30, "B", 10), TSEntry(40, "A", 20))
    )
  }

  @Test def testCompressVectors(): Unit = {
    // Simple cases
    assert(TimeSeries.fitAndCompressTSEntries(Vector()).isEmpty)
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(20, "B", 10)))
        == Vector(TSEntry(20, "B", 10))
    )

    // Test non contiguous entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(10, "A", 9), TSEntry(20, "A", 9), TSEntry(30, "A", 10)))
        == Vector(TSEntry(10, "A", 9), TSEntry(20, "A", 9), TSEntry(30, "A", 10))
    )

    // Test two contiguous and equal entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(10, "A", 10), TSEntry(20, "A", 10)))
        == Vector(TSEntry(10, "A", 20))
    )

    // Test three contiguous and equal entries:
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(10, "A", 10), TSEntry(20, "A", 10), TSEntry(30, "A", 10)))
        == Vector(TSEntry(10, "A", 30))
    )

    // Test four contiguous entries, the two first being unequal
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(10, "B", 10), TSEntry(20, "A", 10), TSEntry(30, "A", 10), TSEntry(40, "A", 10)))
        == Vector(TSEntry(10, "B", 10), TSEntry(20, "A", 30))
    )

    // Test five contiguous entries, the third one being unequal to the others
    assert(
      TimeSeries.fitAndCompressTSEntries(Vector(TSEntry(10, "A", 10), TSEntry(20, "A", 10), TSEntry(30, "B", 10), TSEntry(40, "A", 10), TSEntry(50, "A", 10)))
        == Vector(TSEntry(10, "A", 20), TSEntry(30, "B", 10), TSEntry(40, "A", 20))
    )
  }

  @Test def testCompressionAfterMerge(): Unit = {

    // Perfectly aligned, no discontinuities
    val l =
      Seq(TSEntry(-20, 1.0, 10), TSEntry(-10, 2.0, 10), TSEntry(0, 3.0, 10), TSEntry(10, 1.0, 10), TSEntry(20, 0.0, 10))

    val r =
      Seq(TSEntry(-20, -1.0, 10), TSEntry(-10, -2.0, 10), TSEntry(0, 3.0, 10), TSEntry(10, 1.0, 10), TSEntry(20, 2.0, 10))

    assert(
      TimeSeries.mergeEntries(l)(r)(plus) ==
        Seq(TSEntry(-20, 0.0, 20), TSEntry(0, 6.0, 10), TSEntry(10, 2.0, 20))
    )
  }

  @Test def testFillWithoutCompression(): Unit = {
    // Simple cases: 0 and 1 entries
    assert(TimeSeries.fillGaps(Seq(), 0) == Seq())
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10)), 0) ==
        Seq(TSEntry(1, 1, 10))
    )

    // Two values, contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10))
    )

    // Three values, contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10), TSEntry(21, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 10), TSEntry(21, 3, 10))
    )

    // Two values, non-contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 0, 10), TSEntry(21, 3, 10))
    )

    // Three values, non-contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(41, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 0, 10), TSEntry(21, 2, 10), TSEntry(31, 0, 10), TSEntry(41, 3, 10))
    )

    // Three values, two first non-contiguous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(31, 3, 10)), 0) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 0, 10), TSEntry(21, 2, 10), TSEntry(31, 3, 10))
    )

  }

  @Test def testFillWithCompression(): Unit = {
    // Simple cases: 0 and 1 entries
    assert(TimeSeries.fillGaps(Seq(), 0) == Seq())
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10)), 0) ==
        Seq(TSEntry(1, 1, 10))
    )

    // Two values, non-contiguous, fill value extends previous
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 3, 10)), 1) ==
        Seq(TSEntry(1, 1, 20), TSEntry(21, 3, 10))
    )

    // Two values, non-contiguous, fill value advances next
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 3, 10)), 3) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 3, 20))
    )

    // Two values, non-contiguous, fill value bridges both values
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 1, 10)), 1) ==
        Seq(TSEntry(1, 1, 30))
    )

    // Three values, non-contiguous, extend first and advance last
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(41, 1, 10)), 1) ==
        Seq(TSEntry(1, 1, 20), TSEntry(21, 2, 10), TSEntry(31, 1, 20))
    )

    // Three values, non-contiguous, both advance and extend middle one
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 2, 10), TSEntry(41, 1, 10)), 2) ==
        Seq(TSEntry(1, 1, 10), TSEntry(11, 2, 30), TSEntry(41, 1, 10))
    )

    // Three values, non-contiguous, bridge every one
    assert(
      TimeSeries.fillGaps(Seq(TSEntry(1, 1, 10), TSEntry(21, 1, 10), TSEntry(41, 1, 10)), 1) ==
        Seq(TSEntry(1, 1, 50))
    )
  }

  @Test def testAllDefinitionScenariosMerge(): Unit = {
    val op =
      (left: Option[String], right: Option[String]) =>
        (left, right) match {
          case (Some(a), Some(b)) => Some(s"$a|$b")
          case (None, Some(b))    => Some(s"|$b")
          case (Some(l), None)    => Some(s"$l|")
          case (None, None)       => Some("none")
      }

    val a = Seq(TSEntry(15, "a1", 10), TSEntry(35, "a2", 10))
    val b = Seq(TSEntry(10, "b1", 10), TSEntry(30, "b2", 10))

    assert(
      TimeSeries.mergeEntries(a)(b)(op) ==
        Seq(
          TSEntry(10, "|b1", 5),
          TSEntry(15, "a1|b1", 5),
          TSEntry(20, "a1|", 5),
          TSEntry(25, "none", 5),
          TSEntry(30, "|b2", 5),
          TSEntry(35, "a2|b2", 5),
          TSEntry(40, "a2|", 5)
        )
    )
  }

  @Test def testApplyWithUnsortedEntries(): Unit = {
    val entries = List(
      TSEntry(5, 5, 1),
      TSEntry(1, 1, 1)
    )

    val VectorTimeSeries(data) = TimeSeries(entries)

    assert(data.head == entries.last)
    assert(data.last == entries.head)
  }

  @Test def testApplyShouldFailWithTwoEntriesHavingSameTimestamps(): Unit = {
    assertThrows[IllegalArgumentException](TimeSeries(Seq(TSEntry(1, 1, 1), TSEntry(1, 1, 1))))
  }

  @Test def testAppendPrependWithEmptyShouldBeTheSame(): Unit = {
    val ts = TimeSeries(
      Seq(
        TSEntry(1, 1, 1),
        TSEntry(2, 2, 1)
      )
    )

    assert(ts.append(EmptyTimeSeries) == ts)
    assert(ts.prepend(EmptyTimeSeries) == ts)
  }

  @Test def testAppendPrependWithOutOfDomainShouldReturnArgument(): Unit = {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 1, 1),
        TSEntry(2, 2, 1)
      )
    )

    val ts2 = TimeSeries(
      Seq(
        TSEntry(3, 3, 1),
        TSEntry(4, 4, 1)
      )
    )

    assert(ts2.append(ts1) == ts1)
    assert(ts1.prepend(ts2) == ts2)
  }

  @Test def testAppendPrependShouldTrimIfNeeded(): Unit = {
    val ts1 = TSEntry(0, 1, 10)
    val ts2 = TSEntry(5, 2, 10)

    assert(ts1.append(ts2).entries.head.validity == 5)
    assert(ts2.prepend(ts1).entries.last.timestamp == 10)
  }

  @Test def testAppendPrependShouldCompress(): Unit = {
    val ts1 = TSEntry(1, 1, 1)
    val ts2 = TSEntry(2, 1, 1)

    val result = TSEntry(1, 1, 2)

    assert(ts1.append(ts2) == result)
    assert(ts2.prepend(ts1) == result)
  }

  @Test def testFallbackUncompress(): Unit = {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 'a', 1),
        TSEntry(3, 'c', 1)
      )
    )

    val ts2 = TSEntry(1, 'b', 3)

    assert(
      ts1.fallback(ts2) == TimeSeries(
        Seq(
          TSEntry(1, 'a', 1),
          TSEntry(2, 'b', 1),
          TSEntry(3, 'c', 1)
        )
      )
    )
  }

  @Test def testFallbackCompress(): Unit = {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 'a', 1),
        TSEntry(3, 'a', 1)
      )
    )

    val ts2 = TSEntry(1, 'a', 3)

    assert(ts1.fallback(ts2) == ts2)
  }

  @Test def testDifferentSupportRatio(): Unit = {
    assert(EmptyTimeSeries.supportRatio == 0)
    assert(TSEntry(1, 'a', 123098).supportRatio == 1)

    val ts = TimeSeries(
      Seq(
        TSEntry(0, 'a', 2),
        TSEntry(3, 'a', 1)
      )
    )

    val ts2 = TSEntry(1, 'a', 3)

    assert(ts.supportRatio == 0.75)
  }

  @Test def testMergeEntriesWithUndefinedDomains(): Unit = {
    val ts1 = Seq(
      TSEntry(1, 1, 5),
      TSEntry(10, 2, 10)
    )

    val ts2 = Seq(
      TSEntry(2, 3, 4),
      TSEntry(11, 4, 6)
    )

    val result = TimeSeries.mergeEntries(ts1)(ts2) {
      case (None, None) => Some('Y')
      case _            => None
    }

    assert(result == Vector(TSEntry(6, 'Y', 4)))
  }

  @Test def testMergeEntriesWithUndefinedDomainsButWithStartingValue(): Unit = {
    val ts1 = Seq(
      TSEntry(1, 'a', 2)
    )

    val ts2 = Seq(
      TSEntry(2, 'b', 1),
      TSEntry(5, 'c', 2)
    )

    val result = TimeSeries.mergeEntries[Char, Char, Char](ts1)(ts2) {
      case (Some(v), None) => Some(v)
      case (None, None)    => Some('d')
      case _               => None
    }

    assert(result == Seq(TSEntry(1, 'a', 1), TSEntry(3, 'd', 2)))
  }

  @Test def testMathOperationsCompressedResult(): Unit = {
    val ts1 = TimeSeries(
      Seq(
        TSEntry(1, 0, 1),
        TSEntry(2, 1, 1)
      )
    )

    val ts2 = TimeSeries(
      Seq(
        TSEntry(1, 1, 1),
        TSEntry(2, 0, 1)
      )
    )

    assert(ts1.plus(ts2) == TSEntry(1, 1, 2))
    assert(ts1.minus(ts1) == TSEntry(1, 0, 2))
    assert(ts1.multiply(ts2) == TSEntry(1, 0, 2))
  }

}
