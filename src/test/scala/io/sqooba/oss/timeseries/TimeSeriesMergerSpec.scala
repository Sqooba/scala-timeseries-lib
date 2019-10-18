package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.scalatest.{FlatSpec, Matchers}

class TimeSeriesMergerSpec extends FlatSpec with Matchers {

  // Simple non-strict summing operator
  private def plus(aO: Option[Double], bO: Option[Double]): Option[Double] = {
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a + b)
      case (Some(_), None)    => aO
      case (None, Some(_))    => bO
      case _                  => None
    }
  }

  private def mul(aO: Option[Double], bO: Option[Double]): Option[Double] = {
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a.doubleValue * b.doubleValue)
      case (Some(x), None)    => Some(x)
      case (None, Some(x))    => Some(x)
      case _                  => None
    }
  }

  "TimeSeriesMerger.mergeEntries" should "correctly do SeqMergingSingleToMultiple" in {
    // Single to single within domain.
    val s1 = Seq(TSEntry(1, 2.0, 20))
    val m1 = Seq(TSEntry(5, 1.0, 10))

    TimeSeriesMerger.mergeEntries(s1, m1)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6))

    TimeSeriesMerger.mergeEntries(m1, s1)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6))

    // Merging with two entries wholly contained in the single's domain
    val s3 = Seq(TSEntry(1, 2.0, 20))
    val m3 = Seq(TSEntry(5, 1.0, 5), TSEntry(10, 2.0, 5))

    TimeSeriesMerger.mergeEntries(s3, m3)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6))

    TimeSeriesMerger.mergeEntries(m3, s3)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6))

    val s4 = Seq(TSEntry(1, 2.0, 20))
    val m4 = Seq(TSEntry(5, 1.0, 5), TSEntry(11, 2.0, 5))

    TimeSeriesMerger.mergeEntries(s4, m4)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5))

    TimeSeriesMerger.mergeEntries(m4, s4)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5))

    // Merge with three entries, the first and last one exceeding the single's domain
    val s5 = Seq(TSEntry(1, 2.0, 20))
    val m5 = Seq(TSEntry(0, 1.0, 5), TSEntry(5, 2.0, 5), TSEntry(16, 3.0, 10))

    TimeSeriesMerger.mergeEntries(s5, m5)(plus) shouldBe
      Seq(TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5))

    TimeSeriesMerger.mergeEntries(m5, s5)(plus) shouldBe
      Seq(TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5))

    // Merge with four entries, the first and last one being completely outside of the single's domain
    val s6 = Seq(TSEntry(1, 2.0, 20))
    val m6 = Seq(TSEntry(-10, -1.0, 10), TSEntry(0, 1.0, 5), TSEntry(6, 2.0, 5), TSEntry(16, 3.0, 10), TSEntry(26, 4.0, 10))

    TimeSeriesMerger.mergeEntries(s6, m6)(plus) shouldBe Seq(
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

    TimeSeriesMerger.mergeEntries(s6, m6)(plus) shouldBe Seq(
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

  }

  it should "correctly do ContinuousDomainMerges" in {
    // Perfectly aligned, no discontinuities
    val l1 = Seq(TSEntry(-20, 1.0, 10), TSEntry(-10, 2.0, 10), TSEntry(0, 3.0, 10), TSEntry(10, 4.0, 10))
    val r1 = Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 6.0, 10), TSEntry(0, 7.0, 10), TSEntry(10, 8.0, 10))

    TimeSeriesMerger.mergeEntries(l1, r1)(mul) shouldBe
      Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 12.0, 10), TSEntry(0, 21.0, 10), TSEntry(10, 32.0, 10))

    TimeSeriesMerger.mergeEntries(r1, l1)(mul) shouldBe
      Seq(TSEntry(-20, 5.0, 10), TSEntry(-10, 12.0, 10), TSEntry(0, 21.0, 10), TSEntry(10, 32.0, 10))

    // Shifted
    val r2 = r1.map(e => TSEntry(e.timestamp + 5, e.value, e.validity))

    TimeSeriesMerger.mergeEntries(l1, r2)(mul) shouldBe Seq(
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

    TimeSeriesMerger.mergeEntries(r2, l1)(mul) shouldBe Seq(
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

    // Denser second sequence
    // Perfectly aligned
    val r3 = Seq(
      TSEntry(-20, 5.0, 5),
      TSEntry(-15, 6.0, 5),
      TSEntry(-10, 7.0, 5),
      TSEntry(-5, 8.0, 5),
      TSEntry(0, 9.0, 5),
      TSEntry(5, 10.0, 5),
      TSEntry(10, 11.0, 5),
      TSEntry(15, 12.0, 5)
    )

    TimeSeriesMerger.mergeEntries(l1, r3)(mul) shouldBe Seq(
      TSEntry(-20, 5.0, 5),
      TSEntry(-15, 6.0, 5),
      TSEntry(-10, 14.0, 5),
      TSEntry(-5, 16.0, 5),
      TSEntry(0, 27.0, 5),
      TSEntry(5, 30.0, 5),
      TSEntry(10, 44.0, 5),
      TSEntry(15, 48.0, 5)
    )

    TimeSeriesMerger.mergeEntries(r3, l1)(mul) shouldBe Seq(
      TSEntry(-20, 5.0, 5),
      TSEntry(-15, 6.0, 5),
      TSEntry(-10, 14.0, 5),
      TSEntry(-5, 16.0, 5),
      TSEntry(0, 27.0, 5),
      TSEntry(5, 30.0, 5),
      TSEntry(10, 44.0, 5),
      TSEntry(15, 48.0, 5)
    )

    // Shifted
    val r4 = r3.map(e => TSEntry(e.timestamp + 4, e.value, e.validity))

    TimeSeriesMerger.mergeEntries(l1, r4)(mul) shouldBe Seq(
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

    TimeSeriesMerger.mergeEntries(r4, l1)(mul) shouldBe Seq(
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
  }

  it should "correctly do CompressionAfterMerge" in {

    // Perfectly aligned, no discontinuities
    val l =
      Seq(TSEntry(-20, 1.0, 10), TSEntry(-10, 2.0, 10), TSEntry(0, 3.0, 10), TSEntry(10, 1.0, 10), TSEntry(20, 0.0, 10))

    val r =
      Seq(TSEntry(-20, -1.0, 10), TSEntry(-10, -2.0, 10), TSEntry(0, 3.0, 10), TSEntry(10, 1.0, 10), TSEntry(20, 2.0, 10))

    TimeSeriesMerger.mergeEntries(l, r)(plus) shouldBe
      Seq(TSEntry(-20, 0.0, 20), TSEntry(0, 6.0, 10), TSEntry(10, 2.0, 20))
  }

  it should "correctly do AllDefinitionScenariosMerge" in {
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

    TimeSeriesMerger.mergeEntries(a, b)(op) shouldBe
      Seq(
        TSEntry(10, "|b1", 5),
        TSEntry(15, "a1|b1", 5),
        TSEntry(20, "a1|", 5),
        TSEntry(25, "none", 5),
        TSEntry(30, "|b2", 5),
        TSEntry(35, "a2|b2", 5),
        TSEntry(40, "a2|", 5)
      )

  }

  it should "correctly do MergeEntriesWithUndefinedDomains" in {
    val ts1 = Seq(
      TSEntry(1, 1, 5),
      TSEntry(10, 2, 10)
    )

    val ts2 = Seq(
      TSEntry(2, 3, 4),
      TSEntry(11, 4, 6)
    )

    TimeSeriesMerger.mergeEntries(ts1, ts2) {
      case (None, None) => Some('Y')
      case _            => None
    } shouldBe
      Seq(TSEntry(6, 'Y', 4))
  }

  it should "correctly do MergeEntriesWithUndefinedDomainsButWithStartingValue" in {
    val ts1 = Seq(
      TSEntry(1, 'a', 2)
    )

    val ts2 = Seq(
      TSEntry(2, 'b', 1),
      TSEntry(5, 'c', 2)
    )

    TimeSeriesMerger.mergeEntries[Char, Char, Char](ts1, ts2) {
      case (Some(v), None) => Some(v)
      case (None, None)    => Some('d')
      case _               => None
    } shouldBe
      Seq(TSEntry(1, 'a', 1), TSEntry(3, 'd', 2))
  }

  it should "correctly merge one entry to a seq of entries" in {
    // Single to empty case
    val s0 = TSEntry(1, 2.0, 20)
    val m0 = Seq.empty[TSEntry[Double]]

    TimeSeriesMerger.mergeEntries(Seq(s0), m0)(plus) shouldBe Seq(s0)

    // Simple case, merging to a single entry wholly contained in the domain
    val s1 = TSEntry(1, 2.0, 20)
    val m1 = Seq(TSEntry(5, 1.0, 10))

    TimeSeriesMerger.mergeEntries(Seq(s1), m1)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6))

    // Merging with a single entry that exceeds the single's domain both before and after
    val s2 = TSEntry(5, 2.0, 10)
    val m2 = Seq(TSEntry(1, 1.0, 20))

    TimeSeriesMerger.mergeEntries(Seq(s2), m2)(plus) shouldBe Seq(TSEntry(1, 1.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 1.0, 6))

    // Merging with two entries wholly contained in the single's domain
    val s3 = TSEntry(1, 2.0, 20)
    val m3 = Seq(TSEntry(5, 1.0, 5), TSEntry(10, 2.0, 5))

    TimeSeriesMerger.mergeEntries(Seq(s3), m3)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6))

    val s4 = TSEntry(1, 2.0, 20)
    val m4 = Seq(TSEntry(5, 1.0, 5), TSEntry(11, 2.0, 5))

    TimeSeriesMerger.mergeEntries(Seq(s4), m4)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5))

    // Merge with three entries, the first and last one exceeding the single's domain
    val s5 = TSEntry(1, 2.0, 20)
    val m5 = Seq(TSEntry(0, 1.0, 5), TSEntry(5, 2.0, 5), TSEntry(16, 3.0, 10))

    TimeSeriesMerger.mergeEntries(Seq(s5), m5)(plus) shouldBe
      Seq(TSEntry(0, 1.0, 1), TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5), TSEntry(21, 3.0, 5))

    // Merge with four entries, the first and last one being completely outside of the single's domain
    val s6 = TSEntry(1, 2.0, 20)
    val m6 = Seq(TSEntry(-10, -1.0, 10), TSEntry(1, 1.0, 4), TSEntry(6, 2.0, 5), TSEntry(16, 3.0, 10), TSEntry(26, 4.0, 10))

    TimeSeriesMerger.mergeEntries(Seq(s6), m6)(plus) shouldBe
      Seq(
        TSEntry(-10, -1.0, 10),
        TSEntry(1, 3.0, 4),
        TSEntry(5, 2.0, 1),
        TSEntry(6, 4.0, 5),
        TSEntry(11, 2.0, 5),
        TSEntry(16, 5.0, 5),
        TSEntry(21, 3.0, 5),
        TSEntry(26, 4.0, 10)
      )
  }

  "TimeSeriesMerger.mergeOrdererdSeqs" should "merge ordered sequences" in {

    TimeSeriesMerger.mergeOrderedSeqs(Seq[Int](), Seq[Int]()) shouldBe Seq.empty

    val l1 = Seq(1, 2, 5, 7, 8, 9, 12)
    val l2 = Seq(3, 6, 7, 10, 11, 15, 16)

    TimeSeriesMerger.mergeOrderedSeqs(l1, l2) shouldBe (l1 ++ l2).sorted
  }

  it should "merge two entries" in {
    // Overlapping
    val ao = Seq(TSEntry(1, 2.0, 10))
    val bo = Seq(TSEntry(6, 3.0, 10))

    TimeSeriesMerger.mergeEntries(ao, bo)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 5), TSEntry(6, 5.0, 5), TSEntry(11, 3.0, 5))

    TimeSeriesMerger.mergeEntries(bo, ao)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 5), TSEntry(6, 5.0, 5), TSEntry(11, 3.0, 5))

    // Contiguous
    val ac = Seq(TSEntry(1, 2.0, 10))
    val bc = Seq(TSEntry(11, 3.0, 10))

    TimeSeriesMerger.mergeEntries(ac, bc)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 10), TSEntry(11, 3.0, 10))

    TimeSeriesMerger.mergeEntries(bc, ac)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 10), TSEntry(11, 3.0, 10))

    // Completely separate

    val as = Seq(TSEntry(1, 2.0, 10))
    val bs = Seq(TSEntry(12, 3.0, 10))

    TimeSeriesMerger.mergeEntries(as, bs)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 10), TSEntry(12, 3.0, 10))

    TimeSeriesMerger.mergeEntries(bs, as)(plus) shouldBe
      Seq(TSEntry(1, 2.0, 10), TSEntry(12, 3.0, 10))
  }

  it should "merge an entry to a None with a complex operation" in {

    def op(aO: Option[String], bO: Option[String]): Option[String] =
      (aO, bO) match {
        case (Some(_), None) => aO
        case _               => None
      }

    val t = Seq(TSEntry(1, "Hi", 10))

    TimeSeriesMerger.mergeEntries(t, Seq.empty)(op) shouldBe t
    TimeSeriesMerger.mergeEntries(Seq.empty, t)(op) shouldBe Seq()
  }
}
