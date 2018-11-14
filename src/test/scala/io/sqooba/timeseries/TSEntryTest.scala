package io.sqooba.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry, TSValue, VectorTimeSeries}
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TSEntryTest extends JUnitSuite {

  // Simple summing operator
  def plus(aO: Option[Double], bO: Option[Double]): Option[Double] =
    (aO, bO) match {
      case (Some(a), Some(b)) => Some(a + b)
      case (Some(a), None)    => aO
      case (None, Some(b))    => bO
      case _                  => None
    }

  @Test def testMap(): Unit = {
    assert(TSEntry(0, 42, 10).map(_ / 2) == TSEntry(0, 21, 10))
  }

  @Test def testMapWithTime(): Unit = {
    assert(TSEntry(10, 5, 10).mapWithTime((t, v) => t + v) == TSEntry(10, 15, 10))
  }

  @Test def testFilter: Unit = {
    val t = TSEntry(10, "Hi", 10)
    assert(t.filter(_.validity == 10) == t)
    assert(t.filter(_.timestamp != 10) == EmptyTimeSeries())
    assert(t.filter(_.value == "Hi") == t)
  }

  @Test def testFilterValues: Unit = {
    val t = TSEntry(10, "Hi", 10)
    assert(t.filterValues(_ == "Hi") == t)
    assert(t.filterValues(_ == "Ho") == EmptyTimeSeries())
  }

  @Test def testFill(): Unit = {
    assert(TSEntry(10, 5, 10).fill(42) == TSEntry(10, 5, 10))
  }

  @Test def testAt(): Unit = {
    assert(TSEntry(0, "", 10).at(-1).isEmpty)
    assert(TSEntry(0, "", 10).at(0).contains(""))
    assert(TSEntry(0, "", 10).at(9).contains(""))
    assert(TSEntry(0, "", 10).at(10).isEmpty)
  }

  @Test def testEntryAt(): Unit = {
    assert(TSEntry(0, "", 10).entryAt(-1).isEmpty)
    assert(TSEntry(0, "", 10).entryAt(0).contains(TSEntry(0, "", 10)))
    assert(TSEntry(0, "", 10).entryAt(9).contains(TSEntry(0, "", 10)))
    assert(TSEntry(0, "", 10).entryAt(10).isEmpty)
  }

  @Test def testDefined(): Unit = {
    assert(!TSEntry(0, "", 10).defined(-1))
    assert(TSEntry(0, "", 10).defined(0))
    assert(TSEntry(0, "", 10).defined(9))
    assert(!TSEntry(0, "", 10).defined(10))
  }

  @Test def testDefinedUntil(): Unit = {
    assert(TSEntry(1, "", 10).definedUntil() == 11)
  }

  @Test def testToMapTuple(): Unit = {
    assert(TSEntry(0, "Hi", 10).toMapTuple == (0 -> TSValue("Hi", 10)))
  }

  @Test def testToVal(): Unit = {
    assert(TSEntry(0, "Hi", 10).toVal == TSValue("Hi", 10))
  }

  @Test def testTrimRight(): Unit = {
    val tse = TSEntry(0, "", 10)
    assert(tse.trimRight(10) == tse)
    assert(tse.trimRight(9) == TSEntry(0, "", 9))
    assert(tse.trimRight(1) == TSEntry(0, "", 1))
    assert(tse.trimRight(0) == EmptyTimeSeries())
    assert(tse.trimRight(-1) == EmptyTimeSeries())
  }

  @Test def testTrimEntryRight(): Unit = {
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

  @Test def testTrimLeft(): Unit = {
    val tse = TSEntry(1, "", 10)
    assert(tse.trimLeft(0) == tse)
    assert(tse.trimLeft(1) == tse)
    assert(tse.trimLeft(2) == TSEntry(2, "", 9))
    assert(tse.trimLeft(10) == TSEntry(10, "", 1))
    assert(tse.trimLeft(11) == EmptyTimeSeries())
    assert(tse.trimLeft(12) == EmptyTimeSeries())
  }

  @Test def testTrimEntryLeft(): Unit = {
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

  @Test def testSplit(): Unit = {
    val tse = TSEntry(0, "", 10)
    assert(tse.split(0) == (EmptyTimeSeries(), tse))
    assert(tse.split(1) == (tse.trimEntryRight(1), tse.trimEntryLeft(1)))
    assert(tse.split(5) == (tse.trimEntryRight(5), tse.trimEntryLeft(5)))
    assert(tse.split(9) == (tse.trimEntryRight(9), tse.trimEntryLeft(9)))
    assert(tse.split(10) == (tse, EmptyTimeSeries()))
  }

  @Test def testSlice: Unit = {
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
    assert(EmptyTimeSeries() == t.slice(12, 15))
    assert(EmptyTimeSeries() == t.slice(0, 1))
  }

  @Test def testTrimLeftNRight(): Unit = {
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

  @Test def testOverlaps(): Unit = {
    assert(TSEntry(0, "", 10).overlaps(TSEntry(9, "", 10)))
    assert(TSEntry(9, "", 10).overlaps(TSEntry(0, "", 10)))
    assert(!TSEntry(0, "", 10).overlaps(TSEntry(10, "", 10)))
    assert(!TSEntry(10, "", 10).overlaps(TSEntry(0, "", 10)))
  }

  @Test def testAppendEntryWithoutCompression(): Unit = {
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

  @Test def testAppendEntryWithCompression(): Unit = {
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

  @Test def testPrependEntry(): Unit = {
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

  @Test def testMergeEntriesSimpleOp(): Unit = {

    // For two exactly overlapping entries,
    // result contains a single entry
    val r1 = TSEntry.merge(TSEntry(1, 2.0, 10), TSEntry(1, 3.0, 10))(plus)
    assert(r1.size == 1)
    assert(r1(0) == TSEntry(1, 5.0, 10))

    // Entries don't start at the same time, but have the same end of validity
    val a = TSEntry(1, 2.0, 10)
    val b = TSEntry(6, 3.0, 5)
    assert(a.definedUntil() == b.definedUntil())

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

  @Test def mergeEithersSimpleOp(): Unit = {
    // Overlapping
    val ao = TSEntry(1, 2.0, 10).toLeftEntry[Double]
    val bo = TSEntry(6, 3.0, 10).toRightEntry[Double]

    assert(
      TSEntry.mergeEithers(ao, bo)(plus) ==
        Seq(TSEntry(1, 2.0, 5), TSEntry(6, 5.0, 5), TSEntry(11, 3.0, 5))
    )

    assert(
      TSEntry.mergeEithers(bo, ao)(plus) ==
        Seq(TSEntry(1, 2.0, 5), TSEntry(6, 5.0, 5), TSEntry(11, 3.0, 5))
    )

    // Contiguous
    val ac = TSEntry(1, 2.0, 10).toLeftEntry[Double]
    val bc = TSEntry(11, 3.0, 10).toRightEntry[Double]

    assert(
      TSEntry.mergeEithers(ac, bc)(plus) ==
        Seq(TSEntry(1, 2.0, 10), TSEntry(11, 3.0, 10))
    )

    assert(
      TSEntry.mergeEithers(bc, ac)(plus) ==
        Seq(TSEntry(1, 2.0, 10), TSEntry(11, 3.0, 10))
    )

    // Completely separate

    val as = TSEntry(1, 2.0, 10).toLeftEntry[Double]
    val bs = TSEntry(12, 3.0, 10).toRightEntry[Double]

    assert(
      TSEntry.mergeEithers(as, bs)(plus) ==
        Seq(TSEntry(1, 2.0, 10), TSEntry(12, 3.0, 10))
    )

    assert(
      TSEntry.mergeEithers(bs, as)(plus) ==
        Seq(TSEntry(1, 2.0, 10), TSEntry(12, 3.0, 10))
    )

  }

  @Test def mergeSingleToMultipleSimpleOp(): Unit = {
    // Single to empty case
    val s0 = TSEntry(1, 2.0, 20)
    val m0 = Seq.empty[TSEntry[Double]]

    assert(
      TSEntry.mergeSingleToMultiple(s0.toLeftEntry[Double], m0.map(_.toRightEntry[Double]))(plus) ==
        Seq(s0)
    )

    assert(
      TSEntry.mergeSingleToMultiple(s0.toRightEntry[Double], m0.map(_.toLeftEntry[Double]))(plus) ==
        Seq(s0)
    )

    // Simple case, merging to a single entry wholly contained in the domain
    val s1 = TSEntry(1, 2.0, 20)
    val m1 = Seq(TSEntry(5, 1.0, 10))

    assert(
      TSEntry.mergeSingleToMultiple(s1.toLeftEntry[Double], m1.map(_.toRightEntry[Double]))(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6))
    )

    assert(
      TSEntry.mergeSingleToMultiple(s1.toRightEntry[Double], m1.map(_.toLeftEntry[Double]))(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 10), TSEntry(15, 2.0, 6))
    )

    // Merging with a single entry that exceeds the single's domain both before and after
    val s2 = TSEntry(5, 2.0, 10)
    val m2 = Seq(TSEntry(1, 1.0, 20))

    assert(
      TSEntry.mergeSingleToMultiple(s2.toLeftEntry[Double], m2.map(_.toRightEntry[Double]))(plus) ==
        Seq(TSEntry(5, 3.0, 10))
    )
    assert(
      TSEntry.mergeSingleToMultiple(s2.toRightEntry[Double], m2.map(_.toLeftEntry[Double]))(plus) ==
        Seq(TSEntry(5, 3.0, 10))
    )

    // Merging with two entries wholly contained in the single's domain
    val s3 = TSEntry(1, 2.0, 20)
    val m3 = Seq(TSEntry(5, 1.0, 5), TSEntry(10, 2.0, 5))

    assert(
      TSEntry.mergeSingleToMultiple(s3.toLeftEntry[Double], m3.map(_.toRightEntry[Double]))(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6))
    )

    assert(
      TSEntry.mergeSingleToMultiple(s3.toRightEntry[Double], m3.map(_.toLeftEntry[Double]))(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 4.0, 5), TSEntry(15, 2.0, 6))
    )

    val s4 = TSEntry(1, 2.0, 20)
    val m4 = Seq(TSEntry(5, 1.0, 5), TSEntry(11, 2.0, 5))

    assert(
      TSEntry.mergeSingleToMultiple(s4.toLeftEntry[Double], m4.map(_.toRightEntry[Double]))(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5))
    )

    assert(
      TSEntry.mergeSingleToMultiple(s4.toRightEntry[Double], m4.map(_.toLeftEntry[Double]))(plus) ==
        Seq(TSEntry(1, 2.0, 4), TSEntry(5, 3.0, 5), TSEntry(10, 2.0, 1), TSEntry(11, 4.0, 5), TSEntry(16, 2.0, 5))
    )

    // Merge with three entries, the first and last one exceeding the single's domain
    val s5 = TSEntry(1, 2.0, 20)
    val m5 = Seq(TSEntry(0, 1.0, 5), TSEntry(5, 2.0, 5), TSEntry(16, 3.0, 10))

    assert(
      TSEntry.mergeSingleToMultiple(s5.toLeftEntry[Double], m5.map(_.toRightEntry[Double]))(plus) ==
        Seq(TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5))
    )

    assert(
      TSEntry.mergeSingleToMultiple(s5.toRightEntry[Double], m5.map(_.toLeftEntry[Double]))(plus) ==
        Seq(TSEntry(1, 3.0, 4), TSEntry(5, 4.0, 5), TSEntry(10, 2.0, 6), TSEntry(16, 5.0, 5))
    )

    // Merge with four entries, the first and last one being completely outside of the single's domain
    val s6 = TSEntry(1, 2.0, 20)
    val m6 = Seq(TSEntry(-10, -1.0, 10), TSEntry(1, 1.0, 4), TSEntry(6, 2.0, 5), TSEntry(16, 3.0, 10), TSEntry(26, 4.0, 10))

    assert(
      TSEntry.mergeSingleToMultiple(s6.toLeftEntry[Double], m6.map(_.toRightEntry[Double]))(plus) ==
        Seq(TSEntry(1, 3.0, 4), TSEntry(5, 2.0, 1), TSEntry(6, 4.0, 5), TSEntry(11, 2.0, 5), TSEntry(16, 5.0, 5))
    )

    assert(
      TSEntry.mergeSingleToMultiple(s6.toRightEntry[Double], m6.map(_.toLeftEntry[Double]))(plus) ==
        Seq(TSEntry(1, 3.0, 4), TSEntry(5, 2.0, 1), TSEntry(6, 4.0, 5), TSEntry(11, 2.0, 5), TSEntry(16, 5.0, 5))
    )
  }

  @Test def testMergeEitherToNoneSimpleOp(): Unit = {
    val t = TSEntry(1, 1.0, 10)

    assert(
      TSEntry.mergeEitherToNone(t.toLeftEntry[Double])(plus) ==
        Some(TSEntry(1, 1.0, 10))
    )
    assert(
      TSEntry.mergeEitherToNone(t.toRightEntry[Double])(plus) ==
        Some(TSEntry(1, 1.0, 10))
    )
  }

  @Test def testMergeEitherToNoneComplexOp(): Unit = {
    // Operator...
    // - returning the first (left) operand if the right one is undefined
    // - returning None otherwise.
    def op(aO: Option[String], bO: Option[String]) =
      (aO, bO) match {
        case (Some(a), None) => aO
        case (None, Some(b)) => None
        case _               => None
      }

    val t = TSEntry(1, "Hi", 10)

    assert(
      TSEntry.mergeEitherToNone(t.toLeftEntry[String])(op) ==
        Some(TSEntry(1, "Hi", 10))
    )

    assert(TSEntry.mergeEitherToNone(t.toRightEntry[String])(op) == None)
  }

  @Test def testValidityValidation(): Unit = {
    intercept[IllegalArgumentException] {
      TSEntry(10, "Duh", -1)
    }
    intercept[IllegalArgumentException] {
      TSEntry(10, "Duh", 0)
    }
  }

  @Test def appendEntryTs(): Unit = {
    val e = TSEntry(1, "Hi", 10)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10)) == e.append(TSEntry(12, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) == e.append(TSEntry(11, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Hi", 9), TSEntry(10, "Ho", 10)) == e.append(TSEntry(10, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Hi", 1), TSEntry(2, "Ho", 10)) == e.append(TSEntry(2, "Ho", 10)).entries)
    assert(TSEntry(1, "Ho", 10) == e.append(TSEntry(1, "Ho", 10)))
    assert(TSEntry(0, "Ho", 10) == e.append(TSEntry(0, "Ho", 10)))
  }

  @Test def prependEntryTs(): Unit = {
    val e = TSEntry(1, "Hi", 10)
    assert(Seq(TSEntry(-10, "Ho", 10), TSEntry(1, "Hi", 10)) == e.prepend(TSEntry(-10, "Ho", 10)).entries)
    assert(Seq(TSEntry(-9, "Ho", 10), TSEntry(1, "Hi", 10)) == e.prepend(TSEntry(-9, "Ho", 10)).entries)
    assert(Seq(TSEntry(-8, "Ho", 10), TSEntry(2, "Hi", 9)) == e.prepend(TSEntry(-8, "Ho", 10)).entries)
    assert(Seq(TSEntry(0, "Ho", 10), TSEntry(10, "Hi", 1)) == e.prepend(TSEntry(0, "Ho", 10)).entries)
    assert(Seq(TSEntry(1, "Ho", 10)) == e.prepend(TSEntry(1, "Ho", 10)).entries)
    assert(TSEntry(2, "Ho", 10) == e.prepend(TSEntry(2, "Ho", 10)))
    assert(TSEntry(3, "Ho", 10) == e.prepend(TSEntry(3, "Ho", 10)))
  }

  def testTs(startsAt: Long): VectorTimeSeries[String] = VectorTimeSeries(
    startsAt      -> ("Hi", 10L),
    startsAt + 10 -> ("Ho", 10L),
    // scalastyle:off non.ascii.character.disallowed
    startsAt + 20 -> ("Hé", 10L)
    // scalastyle:on non.ascii.character.disallowed
  )

  @Test def appendTs(): Unit = {
    // Append a multi-entry TS at various times on the entry

    val e = TSEntry(1, "Hu", 10)
    assert(e +: testTs(11).entries == e.append(testTs(11)).entries)
    assert(TSEntry(1, "Hu", 9) +: testTs(10).entries == e.append(testTs(10)).entries)
    assert(TSEntry(1, "Hu", 1) +: testTs(2).entries == e.append(testTs(2)).entries)
    assert(testTs(1).entries == e.append(testTs(1)).entries)
    assert(testTs(0).entries == e.append(testTs(0)).entries)

  }

  @Test def prependTs(): Unit = {
    // Prepend a multi-entry TS at various times on the entry
    val e = TSEntry(1, "Hu", 10)
    assert(testTs(-30).entries :+ e == e.prepend(testTs(-30)).entries)
    assert(testTs(-29).entries :+ TSEntry(1, "Hu", 10) == e.prepend(testTs(-29)).entries)
    assert(testTs(-28).entries :+ TSEntry(2, "Hu", 9) == e.prepend(testTs(-28)).entries)
    assert(testTs(-20).entries :+ TSEntry(10, "Hu", 1) == e.prepend(testTs(-20)).entries)
    assert(testTs(-19).entries == e.prepend(testTs(-19)).entries)
    assert(testTs(-18).entries == e.prepend(testTs(-18)).entries)

  }

  @Test def extendValidity(): Unit = {
    val tse = TSEntry(1, "entry", 10)
    assert(tse.extendValidity(10) == TSEntry(1, "entry", 20))
    assert(tse.extendValidity(0) == tse)

    intercept[IllegalArgumentException] {
      tse.extendValidity(-10)
    }
  }

  @Test def testIntegral(): Unit = {
    assert(TSEntry(0, 1, 1000).integral() == 1.0)
    assert(TSEntry(0, 1, 1).integral(TimeUnit.SECONDS) == 1.0)
    assert(TSEntry(0, 1, 1).integral(TimeUnit.MINUTES) == 60.0)

    assert(TSEntry(0, 1, 1000).integralEntry() == TSEntry(0, 1.0, 1000))
  }

  @Test def testSlidingSum(): Unit = {
    assert(
      TSEntry(1, 42, 10).slidingIntegral(10)
        == TSEntry(1, 42, 10)
    )
  }
}
