package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.{TSEntry, VectorTimeSeries}
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class StrictZipTest extends JUnitSuite {

  @Test def testStrictZip(): Unit = {
    // Perfectly aligned, no discontinuities
    // Mix in some Vector with the Seq to check the correct unapply operator is used.
    val tsA = VectorTimeSeries.ofOrderedEntriesUnsafe(Vector(TSEntry(-10, "A1", 10), TSEntry(0, "A2", 10), TSEntry(10, "A3", 10)))

    val tsB = VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(-10, "B1", 10), TSEntry(0, "B2", 10), TSEntry(10, "B3", 10)))

    assert(
      tsA.strictZip(tsB).entries ==
          Seq(TSEntry(-10, ("A1", "B1"), 10), TSEntry(0, ("A2", "B2"), 10), TSEntry(10, ("A3", "B3"), 10))
    )

    val tsC = VectorTimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(-10, "C1", 10), TSEntry(0, "C2", 10), TSEntry(10, "C3", 10)))

    tsA.strictZip(tsB).strictZip(tsC)
    assert(
      tsA.strictZip(tsB).strictZip(tsC).entries ==
          Seq(TSEntry(-10, (("A1", "B1"), "C1"), 10), TSEntry(0, (("A2", "B2"), "C2"), 10), TSEntry(10, (("A3", "B3"), "C3"), 10))
    )
  }

  @Test def testProdValues(): Unit = {
    // Mix in some Vector with the Seq to check the correct unapply operator is used.
    val vta = TimeSeries.ofOrderedEntriesSafe(Vector(TSEntry(1528943988000L, 468000.0, 660000), TSEntry(1528944588000L, 475000.0, 660000)))
    val vtb = TimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1528943988000L, -468000.0, 660000), TSEntry(1528944588000L, -475000.0, 660000)))

    assert(
      vta.strictZip(vtb).entries ==
          // (We expect the first entry to have been trimmed.)
          Seq(TSEntry(1528943988000L, (468000.0, -468000.0), 600000), TSEntry(1528944588000L, (475000.0, -475000.0), 660000))
    )
  }
}
