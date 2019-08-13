package io.sqooba.public.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.public.timeseries.immutable.{ContiguousTimeDomain, EmptyTimeSeries, TSEntry}
import org.scalatest.{FlatSpec, Matchers}

trait TimeSeriesTestBench extends Matchers { this: FlatSpec =>

  def nonEmptyNonSingletonTimeSeries(
      newTsString: Seq[TSEntry[String]] => TimeSeries[String],
      newTsNumeric: Seq[TSEntry[Double]] => TimeSeries[Double]
  ): Unit = {

    // Two contiguous entries
    val contig2 = newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)))

    // Two entries with a gap in between
    val discon2 = newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10)))

    // Three entries, gap between first and second
    val three = newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)))

    val anotherThree = newTsString(Seq(TSEntry(0, "Hi", 10), TSEntry(10, "Ho", 10), TSEntry(20, "Hu", 10)))

    val tri = newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)))

    it should "give correct values for at()" in {

      // Check two contiguous values
      assert(2 === contig2.size)
      assert(contig2.nonEmpty)
      assert(contig2.at(0).isEmpty)
      assert(contig2.at(1).contains("Hi"))
      assert(contig2.at(10).contains("Hi"))
      assert(contig2.at(11).contains(("Ho")))
      assert(contig2.at(20).contains("Ho"))
      assert(contig2.at(21).isEmpty)

      // Check two non contiguous values
      assert(discon2.size === 2)
      assert(discon2.at(0).isEmpty)
      assert(discon2.at(1).contains("Hi"))
      assert(discon2.at(10).contains("Hi"))
      assert(discon2.at(11).isEmpty)
      assert(discon2.at(12).contains("Ho"))
      assert(discon2.at(21).contains("Ho"))
      assert(discon2.at(22).isEmpty)
    }

    it should "be correctly defined" in {
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

    it should "correctly trim on the left for contiguous entries" in {
      // Two contiguous entries
      // Left of the domain
      assert(contig2 === contig2.trimLeft(0))
      assert(contig2 === contig2.trimLeft(1))

      // Trimming on the first entry
      assert(Seq(TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10)) === contig2.trimLeft(2).entries)
      assert(Seq(TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10)) === contig2.trimLeft(10).entries)

      // Trimming at the boundary between entries:
      assert(Seq(TSEntry(11, "Ho", 10)) === contig2.trimLeft(11).entries)

      // ... and on the second entry:
      assert(Seq(TSEntry(12, "Ho", 9)) === contig2.trimLeft(12).entries)
      assert(Seq(TSEntry(20, "Ho", 1)) === contig2.trimLeft(20).entries)

      // ... and after the second entry:
      assert(contig2.trimLeft(21).isEmpty)
    }

    it should "correctly trim on the left for not contiguous entries" in {
      // Two non-contiguous entries
      // Trimming left of the first entry
      assert(discon2 === discon2.trimLeft(0))
      assert(discon2 === discon2.trimLeft(1))

      // Trimming on the first entry
      assert(Seq(TSEntry(2, "Hi", 9), TSEntry(12, "Ho", 10)) === discon2.trimLeft(2).entries)
      assert(Seq(TSEntry(10, "Hi", 1), TSEntry(12, "Ho", 10)) === discon2.trimLeft(10).entries)

      // Trimming between entries:
      assert(Seq(TSEntry(12, "Ho", 10)) === discon2.trimLeft(11).entries)
      assert(Seq(TSEntry(12, "Ho", 10)) === discon2.trimLeft(12).entries)

      // ... and on the second
      assert(Seq(TSEntry(13, "Ho", 9)) === discon2.trimLeft(13).entries)
      assert(Seq(TSEntry(21, "Ho", 1)) === discon2.trimLeft(21).entries)

      // ... and after the second entry:
      assert(discon2.trimLeft(22).isEmpty)

      // Trim on a three element time series with a discontinuity
      // Left of the first entry
      assert(three === three.trimLeft(0))
      assert(three === three.trimLeft(1))

      // Trimming on the first entry
      assert(
        Seq(TSEntry(2, "Hi", 9), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) ===
          three.trimLeft(2).entries
      )
      assert(
        Seq(TSEntry(10, "Hi", 1), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) ===
          three.trimLeft(10).entries
      )

      // Trimming between entries:
      assert(Seq(TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) === three.trimLeft(11).entries)
      assert(Seq(TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 10)) === three.trimLeft(12).entries)

      // ... and on the second
      assert(Seq(TSEntry(13, "Ho", 9), TSEntry(22, "Ha", 10)) === three.trimLeft(13).entries)
      assert(Seq(TSEntry(21, "Ho", 1), TSEntry(22, "Ha", 10)) === three.trimLeft(21).entries)

      // ... on the border between second and third
      assert(Seq(TSEntry(22, "Ha", 10)) === three.trimLeft(22).entries)
      // on the third
      assert(Seq(TSEntry(23, "Ha", 9)) === three.trimLeft(23).entries)
      assert(Seq(TSEntry(31, "Ha", 1)) === three.trimLeft(31).entries)

      // ... and after every entry.
      assert(three.trimLeft(32).isEmpty)
    }

    it should "correctly trim on the left for discrete entries" in {
      // Two contiguous entries
      // Left of the domain
      assert(contig2 === contig2.trimLeftDiscrete(0, true))
      assert(contig2 === contig2.trimLeftDiscrete(0, false))
      assert(contig2 === contig2.trimLeftDiscrete(1, true))
      assert(contig2 === contig2.trimLeftDiscrete(1, false))

      // Trimming on the first entry
      assert(contig2.entries === contig2.trimLeftDiscrete(2, true).entries)
      assert(Seq(TSEntry(11, "Ho", 10)) === contig2.trimLeftDiscrete(2, false).entries)
      assert(contig2.entries === contig2.trimLeftDiscrete(10, true).entries)
      assert(Seq(TSEntry(11, "Ho", 10)) === contig2.trimLeftDiscrete(2, false).entries)

      // Trimming at the boundary between entries:
      assert(Seq(TSEntry(11, "Ho", 10)) === contig2.trimLeftDiscrete(11, true).entries)
      assert(Seq(TSEntry(11, "Ho", 10)) === contig2.trimLeftDiscrete(11, false).entries)

      // ... and on the second entry:
      assert(Seq(TSEntry(11, "Ho", 10)) === contig2.trimLeftDiscrete(12, true).entries)
      assert(Seq() === contig2.trimLeftDiscrete(12, false).entries)
      assert(Seq(TSEntry(11, "Ho", 10)) === contig2.trimLeftDiscrete(20, true).entries)
      assert(Seq() === contig2.trimLeftDiscrete(20, false).entries)

      // ... and after the second entry:
      assert(contig2.trimLeftDiscrete(21, true).isEmpty)
      assert(contig2.trimLeftDiscrete(21, false).isEmpty)
    }

    it should "correctly trim on the right for contiguous entries" in {

      // Two contiguous entries:
      // Right of the domain:
      assert(contig2.entries === contig2.trimRight(22).entries)
      assert(contig2.entries === contig2.trimRight(21).entries)

      // On the second entry
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9)) === contig2.trimRight(20).entries)
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1)) === contig2.trimRight(12).entries)

      // On the boundary
      assert(Seq(TSEntry(1, "Hi", 10)) === contig2.trimRight(11).entries)

      // On the first entry
      assert(TSEntry(1, "Hi", 9) === contig2.trimRight(10))
      assert(TSEntry(1, "Hi", 1) === contig2.trimRight(2))

      // Before the first entry
      assert(contig2.trimRight(1).isEmpty)
      assert(contig2.trimRight(0).isEmpty)

    }

    it should "correctly trim on the right for not contiguous entries" in {
      // Two non-contiguous entries
      // Trimming right of the second entry
      assert(discon2.entries === discon2.trimRight(23).entries)
      assert(discon2.entries === discon2.trimRight(22).entries)

      // Trimming on the last entry
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 9)) === discon2.trimRight(21).entries)
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 1)) === discon2.trimRight(13).entries)

      // Trimming between entries:
      assert(Seq(TSEntry(1, "Hi", 10)) === discon2.trimRight(12).entries)
      assert(Seq(TSEntry(1, "Hi", 10)) === discon2.trimRight(11).entries)

      // ... and on the first
      assert(Seq(TSEntry(1, "Hi", 9)) === discon2.trimRight(10).entries)
      assert(Seq(TSEntry(1, "Hi", 1)) === discon2.trimRight(2).entries)

      // ... and before the first entry:
      assert(discon2.trimRight(1).isEmpty)
      assert(discon2.trimRight(0).isEmpty)

      // Trim on a three element time series with a discontinuity
      // Right of the last entry
      assert(three.entries === three.trimRight(33).entries)
      assert(three.entries === three.trimRight(32).entries)

      // Trimming on the last entry
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 9)) ===
          three.trimRight(31).entries
      )
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10), TSEntry(22, "Ha", 1)) ===
          three.trimRight(23).entries
      )

      // Trimming between 2nd and 3rd entries:
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 10)) === three.trimRight(22).entries)

      // ... and on the second
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 9)) === three.trimRight(21).entries)
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(12, "Ho", 1)) === three.trimRight(13).entries)

      // ... on the border between 1st and 2nd
      assert(Seq(TSEntry(1, "Hi", 10)) === three.trimRight(12).entries)
      assert(Seq(TSEntry(1, "Hi", 10)) === three.trimRight(11).entries)

      // ... on the first
      assert(Seq(TSEntry(1, "Hi", 9)) === three.trimRight(10).entries)
      assert(Seq(TSEntry(1, "Hi", 1)) === three.trimRight(2).entries)

      // ... and after every entry.
      assert(three.trimRight(1).isEmpty)
      assert(three.trimRight(0).isEmpty)
    }

    it should "correctly trim on the right for discrete entries" in {
      // Two contiguous entries:
      // Right of the domain:
      assert(contig2.entries === contig2.trimRightDiscrete(22, true).entries)
      assert(contig2.entries === contig2.trimRightDiscrete(22, false).entries)
      assert(contig2.entries === contig2.trimRightDiscrete(21, true).entries)
      assert(contig2.entries === contig2.trimRightDiscrete(21, false).entries)

      // On the second entry
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) === contig2.trimRightDiscrete(20, true).entries)
      assert(Seq(TSEntry(1, "Hi", 10)) === contig2.trimRightDiscrete(20, false).entries)
      assert(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) === contig2.trimRightDiscrete(12, true).entries)
      assert(Seq(TSEntry(1, "Hi", 10)) === contig2.trimRightDiscrete(12, false).entries)

      // On the boundary
      assert(Seq(TSEntry(1, "Hi", 10)) === contig2.trimRightDiscrete(11, true).entries)
      assert(Seq(TSEntry(1, "Hi", 10)) === contig2.trimRightDiscrete(11, false).entries)

      // On the first entry
      assert(TSEntry(1, "Hi", 10) === contig2.trimRightDiscrete(10, true))
      assert(contig2.trimRightDiscrete(2, false).isEmpty)

      // Before the first entry
      assert(contig2.trimRightDiscrete(1, true).isEmpty)
      assert(contig2.trimRightDiscrete(1, false).isEmpty)
      assert(contig2.trimRightDiscrete(0, true).isEmpty)
      assert(contig2.trimRightDiscrete(0, false).isEmpty)
    }

    it should "correctly split a timeseries of three entries" in {
      val tri = anotherThree

      assert(tri.split(-1) === (EmptyTimeSeries, tri))
      assert(tri.split(0) === (EmptyTimeSeries, tri))
      assert(tri.split(1) === (tri.trimRight(1), tri.trimLeft(1)))
      assert(tri.split(9) === (tri.trimRight(9), tri.trimLeft(9)))
      assert(tri.split(10) === (tri.trimRight(10), tri.trimLeft(10)))
      assert(tri.split(11) === (tri.trimRight(11), tri.trimLeft(11)))
      assert(tri.split(19) === (tri.trimRight(19), tri.trimLeft(19)))
      assert(tri.split(20) === (tri.trimRight(20), tri.trimLeft(20)))
      assert(tri.split(21) === (tri.trimRight(21), tri.trimLeft(21)))
      assert(tri.split(29) === (tri.trimRight(29), tri.trimLeft(29)))
      assert(tri.split(30) === (tri, EmptyTimeSeries))
      assert(tri.split(31) === (tri, EmptyTimeSeries))
    }

    it should "correctly map a timeseries of three entries" in {
      val tri = anotherThree

      val up = tri.map(s => s.toUpperCase())
      assert(up.size === 3)
      assert(up.at(0).contains("HI"))
      assert(up.at(10).contains("HO"))
      assert(up.at(20).contains("HU"))
    }

    it should "correctly map a timeseries of three entries with compression" in {
      val ts = anotherThree

      val up = ts.map(s => 42, compress = true)
      assert(up.entries === Seq(TSEntry(0, 42, 30)))
    }

    it should "correctly map a timeseries of three entries without compression" in {
      val ts = anotherThree

      val up = ts.map(s => 42, compress = false)
      assert(up.entries === Seq(TSEntry(0, 42, 10), TSEntry(10, 42, 10), TSEntry(20, 42, 10)))
    }

    it should "correctly map with time a timeseries of three entries" in {
      val tri = anotherThree

      val up = tri.mapWithTime((t, s) => s.toUpperCase() + t)
      assert(3 === up.size)
      assert(up.at(0).contains("HI0"))
      assert(up.at(10).contains("HO10"))
      assert(up.at(20).contains("HU20"))
    }

    it should "correctly map with time a timeseries of three entries with compression" in {
      val ts = anotherThree

      val up = ts.mapWithTime((_, _) => 42, compress = true)
      assert(up.entries === Seq(TSEntry(0, 42, 30)))
    }

    it should "correctly map with time a timeseries of three entries without compression" in {
      val ts = anotherThree

      val up = ts.mapWithTime((_, _) => 42, compress = false)
      assert(up.entries === Seq(TSEntry(0, 42, 10), TSEntry(10, 42, 10), TSEntry(20, 42, 10)))
    }

    it should "correctly filter a timeseries of three entries" in {
      val ts = newTsString(Seq(TSEntry(0, "Hi", 10), TSEntry(15, "Ho", 15), TSEntry(30, "Hu", 20)))
      assert(
        ts.filter(_.timestamp < 15) === TSEntry(0, "Hi", 10)
      )
      assert(
        ts.filter(_.validity > 10).entries === Seq(TSEntry(15, "Ho", 15), TSEntry(30, "Hu", 20))
      )
      assert(
        ts.filter(_.value.startsWith("H")).entries === ts.entries
      )
      assert(
        ts.filter(_.value.endsWith("H")) === EmptyTimeSeries
      )
    }

    it should "correctly filter the values of a timeseries of three entries" in {
      val ts = newTsString(Seq(TSEntry(0, "Hi", 10), TSEntry(15, "Ho", 15), TSEntry(30, "Hu", 20)))

      assert(
        ts.filterValues(_.startsWith("H")).entries === ts.entries
      )
      assert(
        ts.filterValues(_.endsWith("H")) === EmptyTimeSeries
      )
    }

    it should "not fill a contiguous timeseries of three entries" in {
      val tri = anotherThree

      assert(tri.fill("Ha") === tri)
    }

    it should "fill a timeseries of three entries" in {
      val tri = newTsString(Seq(TSEntry(0, "Hi", 10), TSEntry(20, "Ho", 10), TSEntry(40, "Hu", 10)))

      assert(
        tri.fill("Ha").entries ===
          Seq(
            TSEntry(0, "Hi", 10),
            TSEntry(10, "Ha", 10),
            TSEntry(20, "Ho", 10),
            TSEntry(30, "Ha", 10),
            TSEntry(40, "Hu", 10)
          )
      )

      assert(
        tri.fill("Hi").entries ===
          Seq(
            TSEntry(0, "Hi", 20),
            TSEntry(20, "Ho", 10),
            TSEntry(30, "Hi", 10),
            TSEntry(40, "Hu", 10)
          )
      )

      assert(
        tri.fill("Ho").entries ===
          Seq(
            TSEntry(0, "Hi", 10),
            TSEntry(10, "Ho", 30),
            TSEntry(40, "Hu", 10)
          )
      )

      assert(
        tri.fill("Hu").entries ===
          Seq(
            TSEntry(0, "Hi", 10),
            TSEntry(10, "Hu", 10),
            TSEntry(20, "Ho", 10),
            TSEntry(30, "Hu", 20)
          )
      )
    }

    it should "return the correct head" in {
      assert(tri.head === TSEntry(1, "Hi", 10))
    }

    it should "return the correct head option" in {
      assert(tri.headOption.contains(TSEntry(1, "Hi", 10)))
    }

    it should "return the correct head value" in {
      assert(tri.headValue === "Hi")
    }

    it should "return the correct head value option" in {
      assert(tri.headValueOption.contains("Hi"))
    }

    it should "return the correct last" in {
      assert(tri.last === TSEntry(22, "Ha", 10))
    }

    it should "return the correct last option" in {
      assert(tri.lastOption.contains(TSEntry(22, "Ha", 10)))
    }

    it should "return the correct last value" in {
      assert(tri.lastValue === "Ha")
    }

    it should "return the correct last value option" in {
      assert(tri.lastValueOption.contains("Ha"))
    }

    it should "append entries correctly" in {
      val tri =
        newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10)))

      // Appending after...
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10), TSEntry(32, "Hy", 10))
          === tri.append(TSEntry(32, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10), TSEntry(31, "Hy", 10))
          === tri.append(TSEntry(31, "Hy", 10)).entries
      )

      // Appending on last entry
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 9), TSEntry(30, "Hy", 10))
          === tri.append(TSEntry(30, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 1), TSEntry(22, "Hy", 10))
          === tri.append(TSEntry(22, "Hy", 10)).entries
      )

      // ... just after and on second entry
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hy", 10))
          === tri.append(TSEntry(21, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9), TSEntry(20, "Hy", 10))
          === tri.append(TSEntry(20, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1), TSEntry(12, "Hy", 10))
          === tri.append(TSEntry(12, "Hy", 10)).entries
      )

      // ... just after and on first
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Hy", 10))
          === tri.append(TSEntry(11, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 9), TSEntry(10, "Hy", 10))
          === tri.append(TSEntry(10, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 1), TSEntry(2, "Hy", 10))
          === tri.append(TSEntry(2, "Hy", 10)).entries
      )

      // And complete override
      assert(
        Seq(TSEntry(1, "Hy", 10))
          === tri.append(TSEntry(1, "Hy", 10)).entries
      )

    }

    it should "prepend entries correctly" in {
      val tri =
        newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10)))

      // Prepending before...
      assert(
        Seq(TSEntry(-10, "Hy", 10), TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(-10, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(-9, "Hy", 10), TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(-9, "Hy", 10)).entries
      )

      // Overlaps with first entry
      assert(
        Seq(TSEntry(-8, "Hy", 10), TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(-8, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(0, "Hy", 10), TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(0, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hy", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(1, "Hy", 10)).entries
      )

      // ... second entry
      assert(
        Seq(TSEntry(2, "Hy", 10), TSEntry(12, "Ho", 9), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(2, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(10, "Hy", 10), TSEntry(20, "Ho", 1), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(10, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(11, "Hy", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(TSEntry(11, "Hy", 10)).entries
      )

      // ... third entry
      assert(
        Seq(TSEntry(12, "Hy", 10), TSEntry(22, "Hu", 9))
          === tri.prepend(TSEntry(12, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(20, "Hy", 10), TSEntry(30, "Hu", 1))
          === tri.prepend(TSEntry(20, "Hy", 10)).entries
      )

      // Complete override
      assert(
        Seq(TSEntry(21, "Hy", 10))
          === tri.prepend(TSEntry(21, "Hy", 10)).entries
      )

      assert(
        Seq(TSEntry(22, "Hy", 10))
          === tri.prepend(TSEntry(22, "Hy", 10)).entries
      )
    }

    def testTs(startsAt: Long): TimeSeries[String] =
      newTsString(
        Seq(
          TSEntry(startsAt, "Ai", 10),
          TSEntry(startsAt + 10, "Ao", 10),
          TSEntry(startsAt + 20, "Au", 10)
        ))

    it should "append a multi-entry TS at various times on the entry" in {
      val tri =
        newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10)))

      // Append after all entries
      assert(tri.entries ++ testTs(31).entries === tri.append(testTs(31)).entries)
      assert(tri.entries ++ testTs(32).entries === tri.append(testTs(32)).entries)

      // On last
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 9)) ++ testTs(30).entries
          === tri.append(testTs(30)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 1)) ++ testTs(22).entries
          === tri.append(testTs(22)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10)) ++ testTs(21).entries
          === tri.append(testTs(21)).entries
      )

      // On second
      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 9)) ++ testTs(20).entries
          === tri.append(testTs(20)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 1)) ++ testTs(12).entries
          === tri.append(testTs(12)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 10)) ++ testTs(11).entries
          === tri.append(testTs(11)).entries
      )

      // On first
      assert(
        Seq(TSEntry(1, "Hi", 9)) ++ testTs(10).entries
          === tri.append(testTs(10)).entries
      )

      assert(
        Seq(TSEntry(1, "Hi", 1)) ++ testTs(2).entries
          === tri.append(testTs(2)).entries
      )

      assert(testTs(1).entries === tri.append(testTs(1)).entries)
      assert(testTs(0).entries === tri.append(testTs(0)).entries)
    }

    it should "prepend a multi-entry TS at various times on the entry" in {
      val tri =
        newTsString(Seq(TSEntry(1, "Hi", 10), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10)))

      // Before all entries
      assert(testTs(-30).entries ++ tri.entries === tri.prepend(testTs(-30)).entries)
      assert(testTs(-29).entries ++ tri.entries === tri.prepend(testTs(-29)).entries)

      // On first
      assert(
        testTs(-28).entries ++ Seq(TSEntry(2, "Hi", 9), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(testTs(-28)).entries
      )

      assert(
        testTs(-20).entries ++ Seq(TSEntry(10, "Hi", 1), TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(testTs(-20)).entries
      )

      assert(
        testTs(-19).entries ++ Seq(TSEntry(11, "Ho", 10), TSEntry(21, "Hu", 10))
          === tri.prepend(testTs(-19)).entries
      )

      // On second
      assert(
        testTs(-18).entries ++ Seq(TSEntry(12, "Ho", 9), TSEntry(21, "Hu", 10))
          === tri.prepend(testTs(-18)).entries
      )

      assert(
        testTs(-10).entries ++ Seq(TSEntry(20, "Ho", 1), TSEntry(21, "Hu", 10))
          === tri.prepend(testTs(-10)).entries
      )

      assert(
        testTs(-9).entries ++ Seq(TSEntry(21, "Hu", 10))
          === tri.prepend(testTs(-9)).entries
      )

      // On third
      assert(
        testTs(-8).entries ++ Seq(TSEntry(22, "Hu", 9))
          === tri.prepend(testTs(-8)).entries
      )

      assert(
        testTs(0).entries ++ Seq(TSEntry(30, "Hu", 1))
          === tri.prepend(testTs(0)).entries
      )

      assert(testTs(1).entries === tri.prepend(testTs(1)).entries)
      assert(testTs(2).entries === tri.prepend(testTs(2)).entries)

    }

    it should "do a step integral" in {
      val tri = newTsNumeric(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10)))

      assert(
        tri.stepIntegral(10, TimeUnit.SECONDS).entries ===
          Seq(TSEntry(0, 10.0, 10), TSEntry(10, 30.0, 10), TSEntry(20, 60.0, 10)))

      val withSampling = TSEntry(0, 1, 30)

      assert(
        withSampling.stepIntegral(10, TimeUnit.SECONDS).entries ===
          Seq(TSEntry(0, 10.0, 10), TSEntry(10, 20.0, 10), TSEntry(20, 30.0, 10))
      )
    }

    it should "split up the entries of a timeseries" in {
      val withSlicing = TSEntry(0, 1, 30)

      assert(
        withSlicing.splitEntriesLongerThan(10).entries === Seq(TSEntry(0, 1, 10), TSEntry(10, 1, 10), TSEntry(20, 1, 10))
      )

      assert(
        withSlicing.splitEntriesLongerThan(20).entries === Seq(TSEntry(0, 1, 20), TSEntry(20, 1, 10))
      )
    }

    it should "split a timeseries into buckets" in {
      val buckets = Stream.from(-10, 10).map(_.toLong)
      val tri     = newTsNumeric(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 5), TSEntry(15, 3, 5)))
      val result  = tri.bucket(buckets)

      val expected = Stream(
        (-10, EmptyTimeSeries),
        (0, TSEntry(0, 1, 10)),
        (10, newTsNumeric(Seq(TSEntry(10, 2, 5), TSEntry(15, 3, 5)))),
        (20, EmptyTimeSeries)
      )

      (expected, result).zipped.foreach {
        case ((eTs, eSeries), (rTs, rSeries)) =>
          rTs shouldBe eTs
          rSeries.entries shouldBe eSeries.entries
      }
    }

    it should "integrate a window of a timeseries between two times" in {
      val tri =
        newTsNumeric(Seq(TSEntry(0, 1, 10), TSEntry(10, 2, 10), TSEntry(20, 3, 10)))

      assert(tri.integrateBetween(-10, 0) === 0)
      assert(tri.integrateBetween(0, 5) === 1)
      assert(tri.integrateBetween(0, 10) === 1)
      assert(tri.integrateBetween(5, 10) === 1)
      assert(tri.integrateBetween(0, 11) === 3)
      assert(tri.integrateBetween(0, 20) === 3)
      assert(tri.integrateBetween(10, 15) === 2)
      assert(tri.integrateBetween(10, 20) === 2)
      assert(tri.integrateBetween(10, 21) === 5)
      assert(tri.integrateBetween(10, 30) === 5)
      assert(tri.integrateBetween(10, 40) === 5)
      assert(tri.integrateBetween(0, 30) === 6)
      assert(tri.integrateBetween(0, 40) === 6)
      assert(tri.integrateBetween(-10, 40) === 6)
    }

    it should "do a sliding integral of a timeseries" in {
      val triA =
        newTsNumeric(
          Seq(
            TSEntry(10, 1, 10),
            TSEntry(21, 2, 2),
            TSEntry(24, 3, 10)
          )
        )

      assert(
        triA.slidingIntegral(1, TimeUnit.SECONDS).entries ===
          Seq(
            TSEntry(10, 10, 11),
            TSEntry(21, 4, 3),
            TSEntry(24, 30, 10)
          )
      )

      assert(
        triA.slidingIntegral(9, TimeUnit.SECONDS).entries ===
          Seq(
            TSEntry(10, 10, 11),
            TSEntry(21, 14, 3),
            TSEntry(24, 44, 5),
            TSEntry(29, 34, 3),
            TSEntry(32, 30, 2)
          )
      )
    }

    it should "return an empty timeseries if one filters all values" in {
      val ts = newTsNumeric(
        Seq(
          TSEntry(1, 1, 1),
          TSEntry(2, 2, 2),
          TSEntry(3, 3, 3)
        )
      )

      assert(ts.filter(_ => false) === EmptyTimeSeries)
    }

    it should "return a correct loose domain" in {
      tri.looseDomain shouldBe ContiguousTimeDomain(tri.head.timestamp, tri.last.definedUntil)
    }

    it should "calculate the support ratio" in {
      val threeFourths = newTsString(Seq(TSEntry(1, "a", 2), TSEntry(4, "b", 1)))
      threeFourths.supportRatio shouldBe 0.75
    }
  }
}
