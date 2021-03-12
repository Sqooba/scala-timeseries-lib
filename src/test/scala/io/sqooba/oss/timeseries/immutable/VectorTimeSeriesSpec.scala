package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeriesTestBench
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class VectorTimeSeriesSpec extends AnyFlatSpec with Matchers with TimeSeriesTestBench {

  "A VectorTimeSeries (unsafe)" should behave like nonEmptyNonSingletonDoubleTimeSeries(
        VectorTimeSeries.ofOrderedEntriesUnsafe(_)
      )

  it should behave like nonEmptyNonSingletonDoubleTimeSeriesWithCompression(
        VectorTimeSeries.ofOrderedEntriesUnsafe(_)
      )

  it should behave like nonEmptyNonSingletonGenericTimeSeries(
        VectorTimeSeries.ofOrderedEntriesUnsafe(_)
      )

  "A VectorTimeSeries (safe)" should behave like nonEmptyNonSingletonDoubleTimeSeries(
        VectorTimeSeries.ofOrderedEntriesSafe(_)
      )

  it should behave like nonEmptyNonSingletonDoubleTimeSeriesWithCompression(
        VectorTimeSeries.ofOrderedEntriesSafe(_)
      )

  it should behave like nonEmptyNonSingletonGenericTimeSeries(
        VectorTimeSeries.ofOrderedEntriesSafe(_)
      )

  "trimRight" should "correctly keep the compressed and continuous flags on continuous entries" in {
    // Two contiguous entries
    // isContinuous = true, isCompressed = true
    val contig2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(11, 222d, 10)))

    // Two contiguous entries:
    assert(contig2.trimRight(22).isCompressed)
    assert(contig2.trimRight(22).isDomainContinuous)

    // On the second entry
    assert(contig2.trimRight(20).isCompressed)
    assert(contig2.trimRight(20).isDomainContinuous)

    // On the boundary
    assert(contig2.trimRight(11).isCompressed)
    assert(contig2.trimRight(11).isDomainContinuous)

    // On the first entry
    assert(contig2.trimRight(10).isCompressed)
    assert(contig2.trimRight(10).isDomainContinuous)
    assert(contig2.trimRight(2).isCompressed)
    assert(contig2.trimRight(2).isDomainContinuous)

    // Before the first entry
    // We expect the timeseries to be Empty, which is NOT compressed and NOT continuous
    assert(contig2.trimRight(1).isCompressed === false)
    assert(contig2.trimRight(0).isDomainContinuous === false)
  }

  it should "correctly keep the compressed and continuous flags on discontinuous entries" in {
    // Two entries with a gap in between
    // isContinuous = false, isCompressed = true
    val discon2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(12, 222d, 10)))
    assert(discon2.isDomainContinuous === false)

    // Right of the domain
    assert(discon2.trimRight(22).isCompressed)
    assert(discon2.trimRight(22).isDomainContinuous === false)

    // On the second entry
    assert(discon2.trimRight(20).isCompressed)
    assert(discon2.trimRight(20).isDomainContinuous === false)

    // On the boundary
    assert(discon2.trimRight(11).isCompressed)
    // This should now be true because we removed the right part
    assert(discon2.trimRight(11).isDomainContinuous)

    // On the first entry
    assert(discon2.trimRight(10).isCompressed)
    assert(discon2.trimRight(10).isDomainContinuous)
    assert(discon2.trimRight(2).isCompressed)
    assert(discon2.trimRight(2).isDomainContinuous)

    // Before the first entry
    // We expect the timeseries to be Empty, which is NOT compressed and NOT continuous
    assert(discon2.trimRight(1).isCompressed === false)
    assert(discon2.trimRight(0).isDomainContinuous === false)
  }

  "trimLeft" should "correctly keep the compressed and continuous flags on contiguous entries" in {
    // Two contiguous entries
    // isContinuous = true, isCompressed = true
    val contig2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(11, 222d, 10)))

    // Two contiguous entries:
    // Left of the domain (that should not change the timeseries):
    assert(contig2.trimLeft(0).isCompressed)
    assert(contig2.trimLeft(0).isDomainContinuous)

    // On the second entry
    assert(contig2.trimLeft(20).isCompressed)
    assert(contig2.trimLeft(20).isDomainContinuous)

    // On the boundary
    assert(contig2.trimLeft(11).isCompressed)
    assert(contig2.trimLeft(11).isDomainContinuous)

    // On the first entry
    assert(contig2.trimLeft(10).isCompressed)
    assert(contig2.trimLeft(10).isDomainContinuous)
    assert(contig2.trimLeft(2).isCompressed)
    assert(contig2.trimLeft(2).isDomainContinuous)
  }

  it should "correctly keep the compressed and continuous flags on noncontiguous entries" in {
    // Two entries with a gap in between
    // isContinuous = false, isCompressed = true
    val discon2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(12, 222d, 10)))
    assert(discon2.isDomainContinuous === false)

    // Left of the domain (that should not change the timeseries):
    assert(discon2.trimLeft(0).isCompressed)
    assert(discon2.trimLeft(0).isDomainContinuous === false)

    // On the second entry
    assert(discon2.trimLeft(20).isCompressed)
    assert(discon2.trimLeft(20).isDomainContinuous)

    // On the boundary
    // Discontinuous entries
    assert(discon2.trimLeft(11).isCompressed)
    // This should now be true because we removed the left part
    assert(discon2.trimLeft(11).isDomainContinuous)

    // On the first entry
    // This only remove the left part, so we still have the gap in between the first and the second TSEntry
    assert(discon2.trimLeft(10).isCompressed)
    assert(discon2.trimLeft(10).isDomainContinuous === false)
    assert(discon2.trimLeft(2).isCompressed)
    assert(discon2.trimLeft(2).isDomainContinuous === false)
  }

  "trimRightDiscrete" should "correctly keep the compressed and continuous flags on continuous entries" in {
    // Two contiguous entries
    // isContinuous = true, isCompressed = true
    val contig2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(11, 222d, 10)))

    // Right of the domain:
    assert(contig2.trimRightDiscrete(22, true).isCompressed)
    assert(contig2.trimRightDiscrete(22, false).isDomainContinuous)

    // On the second entry
    assert(contig2.trimRightDiscrete(20, true).isCompressed)
    assert(contig2.trimRightDiscrete(20, true).isDomainContinuous)

    // On the boundary
    assert(contig2.trimRightDiscrete(11, true).isCompressed)
    assert(contig2.trimRightDiscrete(11, false).isDomainContinuous)

    // On the first entry
    assert(contig2.trimRightDiscrete(10, true).isCompressed)
    assert(contig2.trimRightDiscrete(10, true).isDomainContinuous)
  }

  it should "correctly keep the compressed and continuous flags on discontinuous entries" in {
    // Two contiguous entries
    // isContinuous = false, isCompressed = true
    val discon2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(12, 222d, 10)))
    assert(discon2.isDomainContinuous === false)

    // Right of the domain:
    assert(discon2.trimRightDiscrete(22, true).isCompressed)
    assert(discon2.trimRightDiscrete(22, false).isDomainContinuous === false)

    // On the second entry
    assert(discon2.trimRightDiscrete(20, true).isCompressed)
    assert(discon2.trimRightDiscrete(20, true).isDomainContinuous === false)

    // On the boundary
    assert(discon2.trimRightDiscrete(11, true).isCompressed)
    assert(discon2.trimRightDiscrete(11, false).isDomainContinuous)

    // On the first entry
    assert(discon2.trimRightDiscrete(10, true).isCompressed)
    assert(discon2.trimRightDiscrete(10, true).isDomainContinuous)
  }

  "trimLeftDiscret" should "correctly keep the compressed and continuous flags on contiguous entries" in {
    // Two contiguous entries
    // isContinuous = true, isCompressed = true
    val contig2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(11, 222d, 10)))

    // Two contiguous entries:
    // Left of the domain (that should not change the timeseries):
    assert(contig2.trimLeftDiscrete(0).isCompressed)
    assert(contig2.trimLeftDiscrete(0).isDomainContinuous)

    // On the second entry
    assert(contig2.trimLeftDiscrete(20).isCompressed)
    assert(contig2.trimLeftDiscrete(20).isDomainContinuous)

    // On the boundary
    assert(contig2.trimLeftDiscrete(11).isCompressed)
    assert(contig2.trimLeftDiscrete(11).isDomainContinuous)

    // On the first entry
    assert(contig2.trimLeftDiscrete(10).isCompressed)
    assert(contig2.trimLeftDiscrete(10).isDomainContinuous)
    assert(contig2.trimLeftDiscrete(2).isCompressed)
    assert(contig2.trimLeftDiscrete(2).isDomainContinuous)
  }

  it should "correctly keep the compressed and continuous flags on noncontiguous entries" in {
    // Two entries with a gap in between
    // isContinuous = false, isCompressed = true
    val discon2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 111d, 10), TSEntry(12, 222d, 10)))
    assert(discon2.isDomainContinuous === false)

    // Left of the domain (that should not change the timeseries):
    assert(discon2.trimLeftDiscrete(0).isCompressed)
    assert(discon2.trimLeftDiscrete(0).isDomainContinuous === false)

    // On the second entry
    assert(discon2.trimLeftDiscrete(20).isCompressed)
    assert(discon2.trimLeftDiscrete(20).isDomainContinuous)

    // On the boundary
    // Discontinuous entries
    assert(discon2.trimLeftDiscrete(11).isCompressed)
    // This should now be true because we removed the left part
    assert(discon2.trimLeftDiscrete(11).isDomainContinuous)

    // On the first entry
    // This only remove the left part, so we still have the gap in between the first and the second TSEntry
    assert(discon2.trimLeftDiscrete(10).isCompressed)
    assert(discon2.trimLeftDiscrete(10).isDomainContinuous === false)
    assert(discon2.trimLeftDiscrete(2).isCompressed)
    assert(discon2.trimLeftDiscrete(2).isDomainContinuous === false)
  }

  /*
  TODO: This test is not passing yet
  Here we have three entries, the first two are continuous and the last one is not. When trimming right at the boundary between the
  second and the third entry, the result SHOULD BE continuous (because we removed the gap)
  This is currently not the case because we use the `VectorTimeSeries` constructor in trimRight (or the ofOrderedEntriesUnsafe) which
  does not recompute the domaine.
  I (@gui) don't know what would be the performances implications of using the `ofOrderedEntriesSafe` or having an helper method that could
  determine if a list of entries in a VectorTimeSeries is continuous or not (this should be linear and we could improve performances with an
  early exit in case a gap is found).

  The problem is exactly the same with trimLeft (but with a gap between the first and the second entry, and the second and third entries beeing continuous)
   */
  /* it should "correctly keep the compressed and continuous flags on discontinuous entries" in {
    // Two entries with a gap in between
    // isContinuous = false, isCompressed = true
    val discon2 = VectorTimeSeries.ofOrderedEntriesSafe(Seq(TSEntry(1, 000d, 7), TSEntry(8, 111d, 3), TSEntry(12, 222d, 10)))
    assert(discon2.isDomainContinuous === false)

    // On the second boundary
    assert(discon2.trimRight(11).isCompressed)
    // This should now be true because we removed the right part
    assert(discon2.trimRight(11).isDomainContinuous)
  } */

}
