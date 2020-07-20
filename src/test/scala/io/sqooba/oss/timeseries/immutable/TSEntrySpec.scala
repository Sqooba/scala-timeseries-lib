package io.sqooba.oss.timeseries.immutable

import java.util.concurrent.TimeUnit

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TSEntrySpec extends AnyFlatSpec with should.Matchers {

  // Simple summing operator
  def plus(aO: Option[Double], bO: Option[Double]): Option[Double] =
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a + b)
      case (Some(a), None)    => aO
      case (None, Some(b))    => bO
      case _                  => None
    }

  private val single = TSEntry(1L, "Hi", 10L)

  "TSEntry" should "correctly do Compressed" in {
    assert(TSEntry(1, 2, 3).isCompressed)
  }

  it should "correctly do Continuous" in {
    assert(TSEntry(0, 21, 10).isDomainContinuous)
  }

  it should "correctly do Map" in {
    assert(TSEntry(0, 42, 10).map(_ / 2) == TSEntry(0, 21, 10))
  }

  it should "correctly do MapWithTime" in {
    assert(TSEntry(10, 5, 10).mapWithTime((t, v) => t + v) == TSEntry(10, 15, 10))
  }

  it should "correctly do Filter" in {
    val t = TSEntry(10, "Hi", 10)
    assert(t.filter(_.validity == 10) == t)
    assert(t.filter(_.timestamp != 10) == EmptyTimeSeries)
    assert(t.filter(_.value == "Hi") == t)
  }

  it should "correctly do FilterValues" in {
    val t = TSEntry(10, "Hi", 10)
    assert(t.filterValues(_ == "Hi") == t)
    assert(t.filterValues(_ == "Ho") == EmptyTimeSeries)
  }

  it should "correctly do Fill" in {
    assert(TSEntry(10, 5, 10).fill(42) == TSEntry(10, 5, 10))
  }

  it should "correctly do Head" in {
    assert(TSEntry(10, 5, 10).head == TSEntry(10, 5, 10))
  }

  it should "correctly do HeadOption" in {
    assert(TSEntry(10, 5, 10).headOption == Some(TSEntry(10, 5, 10)))
  }

  it should "correctly do HeadValue" in {
    assert(TSEntry(10, 5, 10).headValue == 5)
  }

  it should "correctly do HeadValueOption" in {
    assert(TSEntry(10, 5, 10).headValueOption == Some(5))
  }

  it should "correctly do Last" in {
    assert(TSEntry(10, 5, 10).last == TSEntry(10, 5, 10))
  }

  it should "correctly do LastOption" in {
    assert(TSEntry(10, 5, 10).lastOption == Some(TSEntry(10, 5, 10)))
  }

  it should "correctly do LastValue" in {
    assert(TSEntry(10, 5, 10).lastValue == 5)
  }

  it should "correctly do LastValueOption" in {
    assert(TSEntry(10, 5, 10).lastValueOption == Some(5))
  }

  it should "correctly do At" in {
    assert(TSEntry(0, "", 10).at(-1).isEmpty)
    assert(TSEntry(0, "", 10).at(0).contains(""))
    assert(TSEntry(0, "", 10).at(9).contains(""))
    assert(TSEntry(0, "", 10).at(10).isEmpty)
  }

  it should "correctly do AtNSize" in {
    assert(1 == single.size)
    assert(single.at(0).isEmpty)
    assert(single.at(1).contains("Hi"))
    assert(single.at(10).contains(("Hi")))
    assert(single.at(11).isEmpty)
  }

  it should "correctly do EntryAt" in {
    assert(TSEntry(0, "", 10).entryAt(-1).isEmpty)
    assert(TSEntry(0, "", 10).entryAt(0).contains(TSEntry(0, "", 10)))
    assert(TSEntry(0, "", 10).entryAt(9).contains(TSEntry(0, "", 10)))
    assert(TSEntry(0, "", 10).entryAt(10).isEmpty)
  }

  it should "correctly do Defined" in {
    assert(!TSEntry(0, "", 10).defined(-1))
    assert(TSEntry(0, "", 10).defined(0))
    assert(TSEntry(0, "", 10).defined(9))
    assert(!TSEntry(0, "", 10).defined(10))

    assert(!single.defined(0))
    assert(single.defined(1))
    assert(single.defined(10))
    assert(!single.defined(11))
  }

  it should "correctly do DefinedUntil" in {
    assert(TSEntry(1, "", 10).definedUntil == 11)
  }

  it should "correctly do TrimRight" in {
    val tse = TSEntry(0, "", 10)
    assert(tse.trimRight(10) == tse)
    assert(tse.trimRight(9) == TSEntry(0, "", 9))
    assert(tse.trimRight(1) == TSEntry(0, "", 1))
    assert(tse.trimRight(0) == EmptyTimeSeries)
    assert(tse.trimRight(-1) == EmptyTimeSeries)

    // Right of the domain:
    assert(single.entries.head == single.trimRight(12))
    assert(single.entries.head == single.trimRight(11))

    // On the entry
    assert(TSEntry(1, "Hi", 9) == single.trimRight(10))
    assert(TSEntry(1, "Hi", 1) == single.trimRight(2))

    // Left of the entry
    assert(EmptyTimeSeries == single.trimRight(1))
    assert(EmptyTimeSeries == single.trimRight(0))
  }

  it should "correctly do TrimRightDiscreteInclude" in {
    val tse = TSEntry(0, "", 10)
    assert(tse.trimRightDiscrete(10, true) == tse)
    assert(tse.trimRightDiscrete(9, true) == tse)
    assert(tse.trimRightDiscrete(1, true) == tse)
    assert(tse.trimRightDiscrete(0, true) == EmptyTimeSeries)
    assert(tse.trimRightDiscrete(-1, true) == EmptyTimeSeries)
  }

  it should "correctly do TrimRightDiscreteExclude" in {
    val tse = TSEntry(0, "", 10)
    assert(tse.trimRightDiscrete(10, false) == tse)
    assert(tse.trimRightDiscrete(9, false) == EmptyTimeSeries)
    assert(tse.trimRightDiscrete(1, false) == EmptyTimeSeries)
    assert(tse.trimRightDiscrete(0, false) == EmptyTimeSeries)
    assert(tse.trimRightDiscrete(-1, false) == EmptyTimeSeries)
  }

  it should "correctly do TrimEntryRight" in {
    val tse = TSEntry(0, "", 10)
    assert(tse.trimEntryRight(10) == tse)
    assert(tse.trimEntryRight(9) == TSEntry(0, "", 9))
    assert(tse.trimEntryRight(1) == TSEntry(0, "", 1))

    intercept[IllegalArgumentException] {
      tse.trimEntryRight(0)
    }
    intercept[IllegalArgumentException] {
      tse.trimEntryRight(-1)
    }
  }

  it should "correctly do TrimLeft" in {
    val tse = TSEntry(1, "", 10)
    assert(tse.trimLeft(0) == tse)
    assert(tse.trimLeft(1) == tse)
    assert(tse.trimLeft(2) == TSEntry(2, "", 9))
    assert(tse.trimLeft(10) == TSEntry(10, "", 1))
    assert(tse.trimLeft(11) == EmptyTimeSeries)
    assert(tse.trimLeft(12) == EmptyTimeSeries)

    assert(single.entries.head == single.trimLeft(0))
    assert(single.entries.head == single.trimLeft(1))

    assert(TSEntry(2, "Hi", 9) == single.trimLeft(2))
    assert(TSEntry(10, "Hi", 1) == single.trimLeft(10))
    assert(EmptyTimeSeries == single.trimLeft(11))
  }

  it should "correctly do TrimLeftDiscreteInclude" in {
    val tse = TSEntry(1, "", 10)
    assert(tse.trimLeftDiscrete(0, true) == tse)
    assert(tse.trimLeftDiscrete(1, true) == tse)
    assert(tse.trimLeftDiscrete(2, true) == tse)
    assert(tse.trimLeftDiscrete(10, true) == tse)
    assert(tse.trimLeftDiscrete(11, true) == EmptyTimeSeries)
    assert(tse.trimLeftDiscrete(12, true) == EmptyTimeSeries)
  }

  it should "correctly do TrimLeftDiscreteExclude" in {
    val tse = TSEntry(1, "", 10)
    assert(tse.trimLeftDiscrete(0, false) == tse)
    assert(tse.trimLeftDiscrete(1, false) == tse)
    assert(tse.trimLeftDiscrete(2, false) == EmptyTimeSeries)
    assert(tse.trimLeftDiscrete(10, false) == EmptyTimeSeries)
    assert(tse.trimLeftDiscrete(11, false) == EmptyTimeSeries)
    assert(tse.trimLeftDiscrete(12, false) == EmptyTimeSeries)
  }

  it should "correctly do TrimEntryLeft" in {
    val tse = TSEntry(1, "", 10)
    assert(tse.trimEntryLeft(0) == tse)
    assert(tse.trimEntryLeft(1) == tse)
    assert(tse.trimEntryLeft(2) == TSEntry(2, "", 9))
    assert(tse.trimEntryLeft(10) == TSEntry(10, "", 1))
    intercept[IllegalArgumentException] {
      tse.trimEntryLeft(11)
    }
    intercept[IllegalArgumentException] {
      tse.trimEntryLeft(12)
    }
  }

  it should "correctly do Split" in {
    val tse = TSEntry(0, "", 10)
    assert(tse.split(0) == (EmptyTimeSeries, tse))
    assert(tse.split(1) == (tse.trimEntryRight(1), tse.trimEntryLeft(1)))
    assert(tse.split(5) == (tse.trimEntryRight(5), tse.trimEntryLeft(5)))
    assert(tse.split(9) == (tse.trimEntryRight(9), tse.trimEntryLeft(9)))
    assert(tse.split(10) == (tse, EmptyTimeSeries))
  }

  it should "correctly do Slice" in {
    val t = TSEntry(1, "Hi", 10)

    // No effect
    assert(TSEntry(1, "Hi", 10) == t.slice(0, 12))
    assert(TSEntry(1, "Hi", 10) == t.slice(1, 11))

    // Check left and right sides.
    assert(TSEntry(2, "Hi", 9) == t.slice(2, 20))
    assert(TSEntry(1, "Hi", 9) == t.slice(0, 10))
    assert(TSEntry(1, "Hi", 4) == t.slice(1, 5))
    assert(TSEntry(5, "Hi", 6) == t.slice(5, 11))

    // Check both
    assert(TSEntry(3, "Hi", 6) == t.slice(3, 9))

    // Outside of definition bounds
    assert(EmptyTimeSeries == t.slice(12, 15))
    assert(EmptyTimeSeries == t.slice(0, 1))
  }

  it should "correctly do TrimLeftNRight" in {
    val t = TSEntry(1, "Hi", 10)

    // No effect
    assert(TSEntry(1, "Hi", 10) == t.trimEntryLeftNRight(0, 12))
    assert(TSEntry(1, "Hi", 10) == t.trimEntryLeftNRight(1, 11))

    // Check left and right sides.
    assert(TSEntry(2, "Hi", 9) == t.trimEntryLeftNRight(2, 20))
    assert(TSEntry(1, "Hi", 9) == t.trimEntryLeftNRight(0, 10))
    assert(TSEntry(1, "Hi", 4) == t.trimEntryLeftNRight(1, 5))
    assert(TSEntry(5, "Hi", 6) == t.trimEntryLeftNRight(5, 11))

    // Check both
    assert(TSEntry(3, "Hi", 6) == t.trimEntryLeftNRight(3, 9))

    // Check exceptions.

    // Outside of bounds
    intercept[IllegalArgumentException] {
      t.trimEntryLeftNRight(12, 15)
    }
    intercept[IllegalArgumentException] {
      t.trimEntryLeftNRight(0, 1)
    }

    // Same or inverted values
    intercept[IllegalArgumentException] {
      t.trimEntryLeftNRight(6, 6)
    }
    intercept[IllegalArgumentException] {
      t.trimEntryLeftNRight(9, 3)
    }

  }

  it should "correctly do Overlaps" in {
    assert(TSEntry(0, "", 10).overlaps(TSEntry(9, "", 10)))
    assert(TSEntry(9, "", 10).overlaps(TSEntry(0, "", 10)))
    assert(!TSEntry(0, "", 10).overlaps(TSEntry(10, "", 10)))
    assert(!TSEntry(10, "", 10).overlaps(TSEntry(0, "", 10)))
  }

  it should "correctly do AppendEntryWithoutCompression" in {
    val tse = TSEntry(1, "Hi", 10)

    // Append without overwrite
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10)) ==
          tse.appendEntry(TSEntry(12, "Ho", 10))
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) ==
          tse.appendEntry(TSEntry(11, "Ho", 10))
    )

    // With partial overwrite
    assert(
      Seq(TSEntry(1, "Hi", 9), TSEntry(10, "Ho", 10)) ==
          tse.appendEntry(TSEntry(10, "Ho", 10))
    )

    assert(
      Seq(TSEntry(1, "Hi", 1), TSEntry(2, "Ho", 10)) ==
          tse.appendEntry(TSEntry(2, "Ho", 10))
    )

    // Complete overwrite
    assert(
      Seq(TSEntry(1, "Ho", 10)) ==
          tse.appendEntry(TSEntry(1, "Ho", 10))
    )

    assert(
      Seq(TSEntry(0, "Ho", 10)) ==
          tse.appendEntry(TSEntry(0, "Ho", 10))
    )

  }

  it should "correctly do AppendEntryWithCompression" in {
    val tse = TSEntry(1, "Hi", 10)

    // Append with a gap in the domain
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Hi", 10)) ==
          tse.appendEntry(TSEntry(12, "Hi", 10))
    )

    // No gap
    // perfect contiguity
    assert(
      Seq(TSEntry(1, "Hi", 20)) ==
          tse.appendEntry(TSEntry(11, "Hi", 10))
    )
    // overlapping domains
    assert(
      Seq(TSEntry(1, "Hi", 14)) ==
          tse.appendEntry(TSEntry(5, "Hi", 10))
    )

    // Complete overwrite
    assert(
      Seq(TSEntry(1, "Hi", 10)) ==
          tse.appendEntry(TSEntry(1, "Hi", 10))
    )

    assert(
      Seq(TSEntry(0, "Hi", 10)) ==
          tse.appendEntry(TSEntry(0, "Hi", 10))
    )

  }

  it should "correctly do AppendEntryWithinDomainAndShorterValidity" in {
    // The appended entry's end of validity ends before the previous end of validity.
    // This is OK and should be like trimming the previous entry at the end of validity of the second.

    // Case where the values are not equal
    assert(
      Seq(TSEntry(1, "Hi", 5), TSEntry(6, "Ho", 2)) ==
          TSEntry(1, "Hi", 10).appendEntry(TSEntry(6, "Ho", 2))
    )

    // Case where the values are equal
    assert(
      Seq(TSEntry(1, "Hi", 7)) ==
          TSEntry(1, "Hi", 10).appendEntry(TSEntry(6, "Hi", 2))
    )
  }

  it should "correctly do PrependEntry" in {
    val tse = TSEntry(11, "Ho", 10)

    // Prepend without overwrite
    assert(
      Seq(TSEntry(0, "Hi", 10), TSEntry(11, "Ho", 10)) ==
          tse.prependEntry(TSEntry(0, "Hi", 10))
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) ==
          tse.prependEntry(TSEntry(1, "Hi", 10))
    )

    // With partial overwrite
    assert(
      Seq(TSEntry(2, "Hi", 10), TSEntry(12, "Ho", 9)) ==
          tse.prependEntry(TSEntry(2, "Hi", 10))
    )

    assert(
      Seq(TSEntry(10, "Hi", 10), TSEntry(20, "Ho", 1)) ==
          tse.prependEntry(TSEntry(10, "Hi", 10))
    )

    // Complete overwrite
    assert(
      Seq(TSEntry(11, "Hi", 10)) ==
          tse.prependEntry(TSEntry(11, "Hi", 10))
    )

    assert(
      Seq(TSEntry(12, "Hi", 10)) ==
          tse.prependEntry(TSEntry(12, "Hi", 10))
    )
  }

  it should "correctly do MergeEntriesSimpleOp" in {

    // For two exactly overlapping entries,
    // result contains a single entry
    val r1 = TSEntry.merge(TSEntry(1, 2.0, 10), TSEntry(1, 3.0, 10))(plus)
    assert(r1.size == 1)
    assert(r1.nonEmpty)
    assert(r1(0) == TSEntry(1, 5.0, 10))

    // Entries don't start at the same time, but have the same end of validity
    val a = TSEntry(1, 2.0, 10)
    val b = TSEntry(6, 3.0, 5)
    assert(a.definedUntil == b.definedUntil)

    // Result should contain two entries: first a, valid until b starts, then the sum.
    val r2 = TSEntry.merge(a, b)(plus)
    assert(r2.size == 2)
    assert(r2(0) == TSEntry(1, 2.0, 5))
    assert(r2(1) == TSEntry(6, 5.0, 5))

    // Should be the same if we inverse the inputs...
    val r3 = TSEntry.merge(b, a)(plus)
    assert(r3.size == 2)
    assert(r3(0) == TSEntry(1, 2.0, 5))
    assert(r3(1) == TSEntry(6, 5.0, 5))

    // Entries start at the same time, but have different end of validity
    val a4 = TSEntry(1, 2.0, 10)
    val b4 = TSEntry(1, 3.0, 5)
    val r4 = TSEntry.merge(a4, b4)(plus)
    assert(r4.size == 2)
    assert(r4(0) == TSEntry(1, 5.0, 5))
    assert(r4(1) == TSEntry(6, 2.0, 5))

    val r5 = TSEntry.merge(b4, a4)(plus)
    assert(r5.size == 2)
    assert(r5(0) == TSEntry(1, 5.0, 5))
    assert(r5(1) == TSEntry(6, 2.0, 5))

    // Two overlapping entries that don't share bounds for their domain of definition:
    // Should result in three different TSEntries
    val a6 = TSEntry(1, 2.0, 10)
    val b6 = TSEntry(6, 3.0, 10)
    val r6 = TSEntry.merge(a6, b6)(plus)
    assert(r6.size == 3)
    assert(r6(0) == TSEntry(1, 2.0, 5))
    assert(r6(1) == TSEntry(6, 5.0, 5))
    assert(r6(2) == TSEntry(11, 3.0, 5))

    // Same for inverted input...
    val r7 = TSEntry.merge(b6, a6)(plus)
    assert(r7.size == 3)
    assert(r7(0) == TSEntry(1, 2.0, 5))
    assert(r7(1) == TSEntry(6, 5.0, 5))
    assert(r7(2) == TSEntry(11, 3.0, 5))

    // Finally, check that non-overlapping entries lead to a seq containing them as-is.
    // obviously not overlapping:
    val r8 = TSEntry.merge(TSEntry(1, 2.0, 10), TSEntry(12, 3.0, 10))(plus)
    assert(r8.size == 2)
    assert(r8(0) == TSEntry(1, 2.0, 10))
    assert(r8(1) == TSEntry(12, 3.0, 10))

    // contiguous but not overlapping: remain untouched as well
    val r9 = TSEntry.merge(TSEntry(1, 2.0, 10), TSEntry(11, 3.0, 10))(plus)
    assert(r9.size == 2)
    assert(r9(0) == TSEntry(1, 2.0, 10))
    assert(r9(1) == TSEntry(11, 3.0, 10))
  }

  it should "correctly do ValidityValidation" in {
    intercept[IllegalArgumentException] {
      TSEntry(10, "Duh", -1)
    }
    intercept[IllegalArgumentException] {
      TSEntry(10, "Duh", 0)
    }
  }

  it should "correctly do appendEntryTs" in {
    val e = TSEntry(1, "Hi", 10)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10)) == e.append(TSEntry(12, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) == e.append(TSEntry(11, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Hi", 9), TSEntry(10, "Ho", 10)) == e.append(TSEntry(10, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Hi", 1), TSEntry(2, "Ho", 10)) == e.append(TSEntry(2, "Ho", 10)).entries)
    assert(TSEntry(1, "Ho", 10) == e.append(TSEntry(1, "Ho", 10)))
    assert(TSEntry(0, "Ho", 10) == e.append(TSEntry(0, "Ho", 10)))
  }

  it should "correctly do prependEntryTs" in {
    val e = TSEntry(1, "Hi", 10)
    assert(Seq(TSEntry(-10, "Ho", 10), TSEntry(1, "Hi", 10)) == e.prepend(TSEntry(-10, "Ho", 10)).entries)
    assert(Seq(TSEntry(-9, "Ho", 10), TSEntry(1, "Hi", 10)) == e.prepend(TSEntry(-9, "Ho", 10)).entries)
    assert(Seq(TSEntry(-8, "Ho", 10), TSEntry(2, "Hi", 9)) == e.prepend(TSEntry(-8, "Ho", 10)).entries)
    assert(Seq(TSEntry(0, "Ho", 10), TSEntry(10, "Hi", 1)) == e.prepend(TSEntry(0, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Ho", 10)) == e.prepend(TSEntry(1, "Ho", 10)).entries)
    assert(TSEntry(2, "Ho", 10) == e.prepend(TSEntry(2, "Ho", 10)))
    assert(TSEntry(3, "Ho", 10) == e.prepend(TSEntry(3, "Ho", 10)))
  }

  def testTs(startsAt: Long): VectorTimeSeries[String] = VectorTimeSeries.ofOrderedEntriesUnsafe(
    Seq(
      TSEntry(startsAt, "Hi", 10L),
      TSEntry(startsAt + 10, "Ho", 10L),
      // scalastyle:off non.ascii.character.disallowed
      TSEntry(startsAt + 20, "Hé", 10L)
      // scalastyle:on non.ascii.character.disallowed
    )
  )

  it should "correctly do appendTs" in {
    // Append a multi-entry TS at various times on the entry

    val e = TSEntry(1, "Hu", 10)
    assert(e +: testTs(11).entries == e.append(testTs(11)).entries)
    assert(TSEntry(1, "Hu", 9) +: testTs(10).entries == e.append(testTs(10)).entries)
    assert(TSEntry(1, "Hu", 1) +: testTs(2).entries == e.append(testTs(2)).entries)
    assert(testTs(1).entries == e.append(testTs(1)).entries)
    assert(testTs(0).entries == e.append(testTs(0)).entries)

  }

  it should "correctly do prependTs" in {
    // Prepend a multi-entry TS at various times on the entry
    val e = TSEntry(1, "Hu", 10)
    assert(testTs(-30).entries :+ e == e.prepend(testTs(-30)).entries)
    assert(testTs(-29).entries :+ TSEntry(1, "Hu", 10) == e.prepend(testTs(-29)).entries)
    assert(testTs(-28).entries :+ TSEntry(2, "Hu", 9) == e.prepend(testTs(-28)).entries)
    assert(testTs(-20).entries :+ TSEntry(10, "Hu", 1) == e.prepend(testTs(-20)).entries)
    assert(testTs(-19).entries == e.prepend(testTs(-19)).entries)
    assert(testTs(-18).entries == e.prepend(testTs(-18)).entries)

  }

  it should "correctly do extendValidity" in {
    val tse = TSEntry(1, "entry", 10)
    assert(tse.extendValidity(10) == TSEntry(1, "entry", 20))
    assert(tse.extendValidity(0) == tse)

    intercept[IllegalArgumentException] {
      tse.extendValidity(-10)
    }
  }

  it should "correctly do Integral" in {
    assert(TSEntry(0, 1, 1000).integral() == 1.0)
    assert(TSEntry(0, 1, 1).integral(TimeUnit.SECONDS) == 1.0)
    assert(TSEntry(0, 1, 1).integral(TimeUnit.MINUTES) == 60.0)

    assert(TSEntry(0, 1, 1000).integralEntry() == TSEntry(0, 1.0, 1000))
  }

  it should "correctly do SlidingSum" in {
    TSEntry(1, 42, 10)
      .slidingIntegral(10, 10, TimeUnit.SECONDS) shouldBe
      TSEntry(1, 10 * 42, 10)

    TSEntry(1, 42, 10)
      .slidingIntegral(10, 1, TimeUnit.SECONDS)
      .entries shouldBe
      (1 to 10).map(i => TSEntry(i, i * 42, 1))
  }

  it should "correctly do SliceEntries" in {
    assert(
      TSEntry(1, 42, 2).splitEntriesLongerThan(1).entries
        == Seq(TSEntry(1, 42, 1), TSEntry(2, 42, 1))
    )
    assert(
      TSEntry(1, 42, 13).splitEntriesLongerThan(6).entries
        == Seq(TSEntry(1, 42, 6), TSEntry(7, 42, 6), TSEntry(13, 42, 1))
    )
    assert(
      TSEntry(1, 42, 100000).splitEntriesLongerThan(1).entries.size
        == 100000
    )
  }

  it should "correctly do Entries" in {
    assert(TSEntry(1, 42, 100000).entries == Seq(TSEntry(1, 42, 100000)))
  }

  it should "correctly do Values" in {
    assert(TSEntry(1, 42, 100000).values == Seq(42))
  }
}
