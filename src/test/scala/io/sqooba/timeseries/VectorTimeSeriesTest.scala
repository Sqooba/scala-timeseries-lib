package io.sqooba.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class VectorTimeSeriesTest extends JUnitSuite {

  val empty = EmptyTimeSeries

  // Single entry
  val single = TSEntry(1L, "Hi", 10L)

  // Two contiguous entries
  val contig2 = VectorTimeSeries(1L -> ("Hi", 10L), 11L -> ("Ho", 10L))

  // Two entries with a gap in between
  val discon2 = VectorTimeSeries(1L -> ("Hi", 10L), 12L -> ("Ho", 10L))

  // Three entries, gap between first and second
  val three = VectorTimeSeries(1L -> ("Hi", 10L), 12L -> ("Ho", 10L), 22L -> ("Ha", 10L))

  @Test def testAtNSize() {
    // Check empty
    assert(0 == empty.size())
    assert(None == empty.at(0))

    // Check single value
    assert(1 == single.size())
    assert(None == single.at(0))
    assert(Some("Hi") == single.at(1))
    assert(Some("Hi") == single.at(10))
    assert(None == single.at(11))

    // Check two contiguous values
    assert(2 == contig2.size())
    assert(contig2.nonEmpty)
    assert(None == contig2.at(0))
    assert(Some("Hi") == contig2.at(1))
    assert(Some("Hi") == contig2.at(10))
    assert(Some("Ho") == contig2.at(11))
    assert(Some("Ho") == contig2.at(20))
    assert(None == contig2.at(21))

    // Check two non contiguous values
    assert(2 == discon2.size())
    assert(None == discon2.at(0))
    assert(Some("Hi") == discon2.at(1))
    assert(Some("Hi") == discon2.at(10))
    assert(None == discon2.at(11))
    assert(Some("Ho") == discon2.at(12))
    assert(Some("Ho") == discon2.at(21))
    assert(None == discon2.at(22))

  }

  @Test def testDefined() {
    // Check empty
    assert(!empty.defined(0))

    // Check single value
    assert(!single.defined(0))
    assert(single.defined(1))
    assert(single.defined(10))
    assert(!single.defined(11))

    // Check two contiguous values
    assert(!contig2.defined(0))
    assert(contig2.defined(1))
    assert(contig2.defined(10))
    assert(contig2.defined(11))
    assert(contig2.defined(20))
    assert(!contig2.defined(21))

    // Check two non contiguous values
    assert(!discon2.defined(0))
    assert(discon2.defined(1))
    assert(discon2.defined(10))
    assert(!discon2.defined(11))
    assert(discon2.defined(12))
    assert(discon2.defined(21))
    assert(!discon2.defined(22))

  }

  @Test def testTrimLeftContiguous() {

    // Trimming an empty TS:
    assert(EmptyTimeSeries == empty.trimLeft(-1))

    // Single entry
    // Trimming left of the domain
    assert(single.entries.head == single.trimLeft(0))
    assert(single.entries.head == single.trimLeft(1))

    // Trimming an entry
    assert(TSEntry(2, "Hi", 9) == single.trimLeft(2))
    assert(TSEntry(10, "Hi", 1) == single.trimLeft(10))
    assert(EmptyTimeSeries == single.trimLeft(11))

    // Two contiguous entries
    // Left of the domain
    assert(contig2 == contig2.trimLeft(0))
    assert(contig2 == contig2.trimLeft(1))

    // Trimming on the first entry
    assert(Seq(TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10)) == contig2.trimLeft(2).entries)
    assert(Seq(TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10)) == contig2.trimLeft(10).entries)

    // Trimming at the boundary between entries:
    assert(Seq(TSEntry(11, "Ho", 10)) == contig2.trimLeft(11).entries)

    // ... and on the second entry:
    assert(Seq(TSEntry(12, "Ho", 9)) == contig2.trimLeft(12).entries)
    assert(Seq(TSEntry(20, "Ho", 1)) == contig2.trimLeft(20).entries)

    // ... and after the second entry:
    assert(EmptyTimeSeries == contig2.trimLeft(21))

  }

  @Test def testTrimLeftDiscontinuous() {
    // Two non-contiguous entries
    // Trimming left of the first entry
    assert(discon2 == discon2.trimLeft(0))
    assert(discon2 == discon2.trimLeft(1))

    // Trimming on the first entry
    assert(Seq(TSEntry(2, "Hi", 9), TSEntry(12, "Ho", 10)) == discon2.trimLeft(2).entries)
    assert(Seq(TSEntry(10, "Hi", 1), TSEntry(12, "Ho", 10)) == discon2.trimLeft(10).entries)

    // Trimming between entries:
    assert(Seq(TSEntry(12, "Ho", 10)) == discon2.trimLeft(11).entries)
    assert(Seq(TSEntry(12, "Ho", 10)) == discon2.trimLeft(12).entries)

    // ... and on the second
    assert(Seq(TSEntry(13, "Ho", 9)) == discon2.trimLeft(13).entries)
    assert(Seq(TSEntry(21, "Ho", 1)) == discon2.trimLeft(21).entries)

    // ... and after the second entry:
    assert(EmptyTimeSeries == discon2.trimLeft(22))

    // Trim on a three element time series with a discontinuity
    // Left of the first entry
    assert(three == three.trimLeft(0))
    assert(three == three.trimLeft(1))

    // Trimming on the first entry
    assert(
      Seq(TSEntry(2, "Hi", 9), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) ==
        three.trimLeft(2).entries
    )
    assert(
      Seq(TSEntry(10, "Hi", 1), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) ==
        three.trimLeft(10).entries
    )

    // Trimming between entries:
    assert(Seq(TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) == three.trimLeft(11).entries)
    assert(Seq(TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) == three.trimLeft(12).entries)

    // ... and on the second
    assert(Seq(TSEntry(13, "Ho", 9), TSEntry(22, "Ha", 10)) == three.trimLeft(13).entries)
    assert(Seq(TSEntry(21, "Ho", 1), TSEntry(22, "Ha", 10)) == three.trimLeft(21).entries)

    // ... on the border between second and third
    assert(Seq(TSEntry(22, "Ha", 10)) == three.trimLeft(22).entries)
    // on the third
    assert(Seq(TSEntry(23, "Ha", 9)) == three.trimLeft(23).entries)
    assert(Seq(TSEntry(31, "Ha", 1)) == three.trimLeft(31).entries)

    // ... and after every entry.
    assert(EmptyTimeSeries == three.trimLeft(32))
  }

  @Test def testTrimRightContiguous() {
    // empty case...
    assert(EmptyTimeSeries == empty.trimRight(0))

    // Single entry:
    // Right of the domain:
    assert(single.entries.head == single.trimRight(12))
    assert(single.entries.head == single.trimRight(11))

    // On the entry
    assert(TSEntry(1, "Hi", 9) == single.trimRight(10))
    assert(TSEntry(1, "Hi", 1) == single.trimRight(2))

    // Left of the entry
    assert(EmptyTimeSeries == single.trimRight(1))
    assert(EmptyTimeSeries == single.trimRight(0))

    // Two contiguous entries:
    // Right of the domain:
    assert(contig2 == contig2.trimRight(22))
    assert(contig2 == contig2.trimRight(21))

    // On the second entry
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9)) == contig2.trimRight(20).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1)) == contig2.trimRight(12).entries)

    // On the boundary
    assert(Seq(TSEntry(1, "Hi", 10)) == contig2.trimRight(11).entries)

    // On the first entry
    assert(TSEntry(1, "Hi", 9) == contig2.trimRight(10))
    assert(TSEntry(1, "Hi", 1) == contig2.trimRight(2))

    // Before the first entry
    assert(EmptyTimeSeries == contig2.trimRight(1))
    assert(EmptyTimeSeries == contig2.trimRight(0))

  }

  @Test def testTrimRightDiscontinuous() {
    // Two non-contiguous entries
    // Trimming right of the second entry
    assert(discon2 == discon2.trimRight(23))
    assert(discon2 == discon2.trimRight(22))

    // Trimming on the last entry
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 9)) == discon2.trimRight(21).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 1)) == discon2.trimRight(13).entries)

    // Trimming between entries:
    assert(Seq(TSEntry(1, "Hi", 10)) == discon2.trimRight(12).entries)
    assert(Seq(TSEntry(1, "Hi", 10)) == discon2.trimRight(11).entries)

    // ... and on the first
    assert(Seq(TSEntry(1, "Hi", 9)) == discon2.trimRight(10).entries)
    assert(Seq(TSEntry(1, "Hi", 1)) == discon2.trimRight(2).entries)

    // ... and before the first entry:
    assert(EmptyTimeSeries == discon2.trimRight(1))
    assert(EmptyTimeSeries == discon2.trimRight(0))

    // Trim on a three element time series with a discontinuity
    // Right of the last entry
    assert(three == three.trimRight(33))
    assert(three == three.trimRight(32))

    // Trimming on the last entry
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 9)) ==
        three.trimRight(31).entries
    )
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 1)) ==
        three.trimRight(23).entries
    )

    // Trimming between 2nd and 3rd entries:
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10)) == three.trimRight(22).entries)

    // ... and on the second
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 9)) == three.trimRight(21).entries)
    assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 1)) == three.trimRight(13).entries)

    // ... on the border between 1st and 2nd
    assert(Seq(TSEntry(1, "Hi", 10)) == three.trimRight(12).entries)
    assert(Seq(TSEntry(1, "Hi", 10)) == three.trimRight(11).entries)

    // ... on the first
    assert(Seq(TSEntry(1, "Hi", 9)) == three.trimRight(10).entries)
    assert(Seq(TSEntry(1, "Hi", 1)) == three.trimRight(2).entries)

    // ... and after every entry.
    assert(EmptyTimeSeries == three.trimRight(1))
    assert(EmptyTimeSeries == three.trimRight(0))
  }

  @Test def testSplit() {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))
    assert(tri.split(-1) == (EmptyTimeSeries, tri))
    assert(tri.split(0) == (EmptyTimeSeries, tri))
    assert(tri.split(1) == (tri.trimRight(1), tri.trimLeft(1)))
    assert(tri.split(9) == (tri.trimRight(9), tri.trimLeft(9)))
    assert(tri.split(10) == (tri.trimRight(10), tri.trimLeft(10)))
    assert(tri.split(11) == (tri.trimRight(11), tri.trimLeft(11)))
    assert(tri.split(19) == (tri.trimRight(19), tri.trimLeft(19)))
    assert(tri.split(20) == (tri.trimRight(20), tri.trimLeft(20)))
    assert(tri.split(21) == (tri.trimRight(21), tri.trimLeft(21)))
    assert(tri.split(29) == (tri.trimRight(29), tri.trimLeft(29)))
    assert(tri.split(30) == (tri, EmptyTimeSeries))
    assert(tri.split(31) == (tri, EmptyTimeSeries))
  }

  @Test def testMap() {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    val up = tri.map(s => s.toUpperCase())
    assert(3 == up.size())
    assert(up.at(0) == Some("HI"))
    assert(up.at(10) == Some("HO"))
    assert(up.at(20) == Some("HU"))
  }

  @Test def testMapWithCompressionFlag(): Unit = {
    val ts =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    val up = ts.map(s => 42, true)
    assert(up.entries == Seq(TSEntry[Int](0, 42, 30)))
  }

  @Test def testMapWithoutCompressionFlag(): Unit = {
    val ts =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    val up = ts.map(s => 42, false)
    assert(up.entries == Seq(TSEntry[Int](0, 42, 10), TSEntry[Int](10, 42, 10), TSEntry[Int](20, 42, 10)))
  }

  @Test def testMapWithTime() {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    val up = tri.mapWithTime((t, s) => s.toUpperCase() + t)
    assert(3 == up.size())
    assert(up.at(0) == Some("HI0"))
    assert(up.at(10) == Some("HO10"))
    assert(up.at(20) == Some("HU20"))
  }

  @Test def testMapWithTimeWithCompressionFlag(): Unit = {
    val ts =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    val up = ts.mapWithTime((_, _) => 42, true)
    assert(up.entries == Seq(TSEntry[Int](0, 42, 30)))
  }

  @Test def testMapWithTimeWithoutCompressionFlag(): Unit = {
    val ts =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    val up = ts.mapWithTime((_, _) => 42, false)
    assert(up.entries == Seq(TSEntry[Int](0, 42, 10), TSEntry[Int](10, 42, 10), TSEntry[Int](20, 42, 10)))
  }

  @Test def testFilter: Unit = {
    val ts =
      VectorTimeSeries(0L -> ("Hi", 10L), 15L -> ("Ho", 15L), 30L -> ("Hu", 20L))
    assert(
      ts.filter(_.timestamp < 15) ==
        TSEntry(0L, "Hi", 10L)
    )
    assert(
      ts.filter(_.validity > 10) ==
        VectorTimeSeries(15L -> ("Ho", 15L), 30L -> ("Hu", 20L))
    )
    assert(
      ts.filter(_.value.startsWith("H")) == ts
    )
    assert(
      ts.filter(_.value.endsWith("H")) == EmptyTimeSeries
    )
  }

  @Test def testFilterValues: Unit = {
    val ts =
      VectorTimeSeries(0L -> ("Hi", 10L), 15L -> ("Ho", 15L), 30L -> ("Hu", 20L))

    assert(
      ts.filterValues(_.startsWith("H")) == ts
    )
    assert(
      ts.filterValues(_.endsWith("H")) == EmptyTimeSeries
    )
  }

  @Test def testFillContiguous(): Unit = {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 10L -> ("Ho", 10L), 20L -> ("Hu", 10L))

    assert(tri.fill("Ha") == tri)
  }

  @Test def testFill(): Unit = {
    val tri =
      VectorTimeSeries(0L -> ("Hi", 10L), 20L -> ("Ho", 10L), 40L -> ("Hu", 10L))

    assert(
      tri.fill("Ha") ==
        VectorTimeSeries(
          0L  -> ("Hi", 10L),
          10L -> ("Ha", 10L),
          20L -> ("Ho", 10L),
          30L -> ("Ha", 10L),
          40L -> ("Hu", 10L)
        )
    )

    assert(
      tri.fill("Hi") ==
        VectorTimeSeries(
          0L  -> ("Hi", 20L),
          20L -> ("Ho", 10L),
          30L -> ("Hi", 10L),
          40L -> ("Hu", 10L)
        )
    )

    assert(
      tri.fill("Ho") ==
        VectorTimeSeries(
          0L  -> ("Hi", 10L),
          10L -> ("Ho", 30L),
          40L -> ("Hu", 10L)
        )
    )

    assert(
      tri.fill("Hu") ==
        VectorTimeSeries(
          0L  -> ("Hi", 10L),
          10L -> ("Hu", 10L),
          20L -> ("Ho", 10L),
          30L -> ("Hu", 20L)
        )
    )
  }

  val tri = VectorTimeSeries(1L -> ("Hi", 10L), 12L -> ("Ho", 10L), 22L -> ("Ha", 10L))

  @Test def testHead: Unit = {
    assert(tri.head == TSEntry(1, "Hi", 10))
  }

  @Test def testHeadOption: Unit = {
    assert(tri.headOption == Some(TSEntry(1, "Hi", 10)))
  }

  @Test def testHeadValue: Unit = {
    assert(tri.headValue == "Hi")
  }

  @Test def testHeadValueOption: Unit = {
    assert(tri.headValueOption == Some("Hi"))
  }

  @Test def testLast: Unit = {
    assert(tri.last == TSEntry(22, "Ha", 10))
  }

  @Test def testLastOption: Unit = {
    assert(tri.lastOption == Some(TSEntry(22, "Ha", 10)))
  }

  @Test def testLastValue: Unit = {
    assert(tri.lastValue == "Ha")
  }

  @Test def testLastValueOption: Unit = {
    assert(tri.lastValueOption == Some("Ha"))
  }

  @Test def appendEntry() {
    val tri =
      VectorTimeSeries(1L -> ("Hi", 10L), 11L -> ("Ho", 10L), 21L -> ("Hu", 10L))

    // Appending after...
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10), TSEntry(32, "Hy", 10))
        == tri.append(TSEntry(32, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10), TSEntry(31, "Hy", 10))
        == tri.append(TSEntry(31, "Hy", 10)).entries
    )

    // Appending on last entry
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 9), TSEntry(30, "Hy", 10))
        == tri.append(TSEntry(30, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 1), TSEntry(22, "Hy", 10))
        == tri.append(TSEntry(22, "Hy", 10)).entries
    )

    // ... just after and on second entry
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hy", 10))
        == tri.append(TSEntry(21, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9), TSEntry(20, "Hy", 10))
        == tri.append(TSEntry(20, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1), TSEntry(12, "Hy", 10))
        == tri.append(TSEntry(12, "Hy", 10)).entries
    )

    // ... just after and on first
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Hy", 10))
        == tri.append(TSEntry(11, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 9), TSEntry(10, "Hy", 10))
        == tri.append(TSEntry(10, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 1), TSEntry(2, "Hy", 10))
        == tri.append(TSEntry(2, "Hy", 10)).entries
    )

    // And complete override
    assert(
      Seq(TSEntry(1, "Hy", 10))
        == tri.append(TSEntry(1, "Hy", 10)).entries
    )

  }

  @Test def prependEntry() {
    val tri =
      VectorTimeSeries(1L -> ("Hi", 10L), 11L -> ("Ho", 10L), 21L -> ("Hu", 10L))

    // Prepending before...
    assert(
      Seq(TSEntry(-10, "Hy", 10), TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(-10, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(-9, "Hy", 10), TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(-9, "Hy", 10)).entries
    )

    // Overlaps with first entry
    assert(
      Seq(TSEntry(-8, "Hy", 10), TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(-8, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(0, "Hy", 10), TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(0, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hy", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(1, "Hy", 10)).entries
    )

    // ... second entry
    assert(
      Seq(TSEntry(2, "Hy", 10), TSEntry(12, "Ho", 9), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(2, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(10, "Hy", 10), TSEntry(20, "Ho", 1), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(10, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(11, "Hy", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(TSEntry(11, "Hy", 10)).entries
    )

    // ... third entry
    assert(
      Seq(TSEntry(12, "Hy", 10), TSEntry(22, "Hu", 9))
        == tri.prepend(TSEntry(12, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(20, "Hy", 10), TSEntry(30, "Hu", 1))
        == tri.prepend(TSEntry(20, "Hy", 10)).entries
    )

    // Complete override
    assert(
      Seq(TSEntry(21, "Hy", 10))
        == tri.prepend(TSEntry(21, "Hy", 10)).entries
    )

    assert(
      Seq(TSEntry(22, "Hy", 10))
        == tri.prepend(TSEntry(22, "Hy", 10)).entries
    )
  }

  def testTs(startsAt: Long): VectorTimeSeries[String] = VectorTimeSeries(
    startsAt      -> ("Ai", 10L),
    startsAt + 10 -> ("Ao", 10L),
    startsAt + 20 -> ("Au", 10L)
  )

  @Test def appendTs() {
    // Append a multi-entry TS at various times on the entry

    val tri =
      VectorTimeSeries(1L -> ("Hi", 10L), 11L -> ("Ho", 10L), 21L -> ("Hu", 10L))

    // Append after all entries
    assert(tri.entries ++ testTs(31).entries == tri.append(testTs(31)).entries)
    assert(tri.entries ++ testTs(32).entries == tri.append(testTs(32)).entries)

    // On last
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 9)) ++ testTs(30).entries
        == tri.append(testTs(30)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 1)) ++ testTs(22).entries
        == tri.append(testTs(22)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) ++ testTs(21).entries
        == tri.append(testTs(21)).entries
    )

    // On second
    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9)) ++ testTs(20).entries
        == tri.append(testTs(20)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1)) ++ testTs(12).entries
        == tri.append(testTs(12)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 10)) ++ testTs(11).entries
        == tri.append(testTs(11)).entries
    )

    // On first
    assert(
      Seq(TSEntry(1, "Hi", 9)) ++ testTs(10).entries
        == tri.append(testTs(10)).entries
    )

    assert(
      Seq(TSEntry(1, "Hi", 1)) ++ testTs(2).entries
        == tri.append(testTs(2)).entries
    )

    assert(testTs(1).entries == tri.append(testTs(1)).entries)
    assert(testTs(0).entries == tri.append(testTs(0)).entries)
  }

  @Test def prependTs() {
    // Prepend a multi-entry TS at various times on the entry
    val tri =
      VectorTimeSeries(1L -> ("Hi", 10L), 11L -> ("Ho", 10L), 21L -> ("Hu", 10L))

    // Before all entries
    assert(testTs(-30).entries ++ tri.entries == tri.prepend(testTs(-30)).entries)
    assert(testTs(-29).entries ++ tri.entries == tri.prepend(testTs(-29)).entries)

    // On first
    assert(
      testTs(-28).entries ++ Seq(TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-28)).entries
    )

    assert(
      testTs(-20).entries ++ Seq(TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-20)).entries
    )

    assert(
      testTs(-19).entries ++ Seq(TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-19)).entries
    )

    // On second
    assert(
      testTs(-18).entries ++ Seq(TSEntry(12, "Ho", 9), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-18)).entries
    )

    assert(
      testTs(-10).entries ++ Seq(TSEntry(20, "Ho", 1), TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-10)).entries
    )

    assert(
      testTs(-9).entries ++ Seq(TSEntry(21, "Hu", 10))
        == tri.prepend(testTs(-9)).entries
    )

    // On third
    assert(
      testTs(-8).entries ++ Seq(TSEntry(22, "Hu", 9))
        == tri.prepend(testTs(-8)).entries
    )

    assert(
      testTs(0).entries ++ Seq(TSEntry(30, "Hu", 1))
        == tri.prepend(testTs(0)).entries
    )

    assert(testTs(1).entries == tri.prepend(testTs(1)).entries)
    assert(testTs(2).entries == tri.prepend(testTs(2)).entries)

  }

  @Test def testStepIntegral(): Unit = {
    val tri =
      VectorTimeSeries(0L -> (1, 10L), 10L -> (2, 10L), 20L -> (3, 10L))

    tri.stepIntegral(10, TimeUnit.SECONDS).entries ==
      Seq(TSEntry(0, 10.0, 10), TSEntry(10, 30.0, 10), TSEntry(20, 60.0, 10))

    val withSampling = TSEntry(0L, 1, 30L)

    assert(
      withSampling.stepIntegral(10, TimeUnit.SECONDS).entries ==
        Seq(TSEntry(0, 10.0, 10), TSEntry(10, 20.0, 10), TSEntry(20, 30.0, 10))
    )
  }

  @Test def testResampling(): Unit = {
    val withSampling = TSEntry(0L, 1, 30L)

    assert(
      withSampling.resample(10).entries == Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10), TSEntry(20, 1, 10))
    )

    assert(
      withSampling.resample(20).entries == Seq(TSEntry(0, 1, 20), TSEntry(20, 1, 10))
    )
  }

  @Test def testIntegrateBetween(): Unit = {
    val tri =
      VectorTimeSeries(0L -> (1, 10L), 10L -> (2, 10L), 20L -> (3, 10L))

    assert(tri.integrateBetween(-10, 0) == 0)
    assert(tri.integrateBetween(0, 5) == 1)
    assert(tri.integrateBetween(0, 10) == 1)
    assert(tri.integrateBetween(5, 10) == 1)
    assert(tri.integrateBetween(0, 11) == 3)
    assert(tri.integrateBetween(0, 20) == 3)
    assert(tri.integrateBetween(10, 15) == 2)
    assert(tri.integrateBetween(10, 20) == 2)
    assert(tri.integrateBetween(10, 21) == 5)
    assert(tri.integrateBetween(10, 30) == 5)
    assert(tri.integrateBetween(10, 40) == 5)
    assert(tri.integrateBetween(0, 30) == 6)
    assert(tri.integrateBetween(0, 40) == 6)
    assert(tri.integrateBetween(-10, 40) == 6)
  }

  @Test def testSlidingIntegral(): Unit = {

    val triA =
      VectorTimeSeries.ofEntriesSafe(
        Seq(
          TSEntry(10, 1, 10),
          TSEntry(21, 2, 2),
          TSEntry(24, 3, 10)
        )
      )

    assert(
      triA.slidingIntegral(2, TimeUnit.SECONDS) ==
        VectorTimeSeries.ofEntriesSafe(
          Seq(
            TSEntry(10, 10, 11),
            TSEntry(21, 4, 3),
            TSEntry(24, 30, 10)
          )
        )
    )

    assert(
      triA.slidingIntegral(10, TimeUnit.SECONDS) ==
        VectorTimeSeries.ofEntriesSafe(
          Seq(
            TSEntry(10, 10, 11),
            TSEntry(21, 14, 3),
            TSEntry(24, 44, 5),
            TSEntry(29, 34, 3),
            TSEntry(32, 30, 2)
          )
        )
    )
  }

  @Test def testFilterAllValuesShouldReturnAnEmptyTimeSeries: Unit = {
    val ts = VectorTimeSeries.ofEntriesSafe(
      Seq(
        TSEntry(1, 1, 1),
        TSEntry(2, 2, 2),
        TSEntry(3, 3, 3)
      )
    )

    assert(ts.filter(_ => false) == EmptyTimeSeries)
  }

  // TODO add test for constructor using the 'ofEntriesSafe' function.
}
