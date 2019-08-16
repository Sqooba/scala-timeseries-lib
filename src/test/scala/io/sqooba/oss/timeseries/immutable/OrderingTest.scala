package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeries
import org.junit.Test
import org.scalatest.junit.JUnitSuite

/**
  * Test implicit ordering definition and the (hopefully) efficient
  * merging of ordered collections of time series entries.
  */
class OrderingTest extends JUnitSuite {

  @Test def testImplicitOrdering() {
    val unOrd = Seq(TSEntry(11, "B", 10), TSEntry(1, "A", 10))

    assert(Seq(TSEntry(1, "A", 10), TSEntry(11, "B", 10)) != unOrd)

    assert(Seq(TSEntry(1, "A", 10), TSEntry(11, "B", 10)) == unOrd.sorted)

  }

  @Test def testMergeOrderedSeqs() {

    assert(Seq() == TimeSeries.mergeOrderedSeqs(Seq[Int](), Seq[Int]()))

    val l1 = Seq(1, 2, 5, 7, 8, 9, 12)
    val l2 = Seq(3, 6, 7, 10, 11, 15, 16)
    val m  = (l1 ++ l2).sorted

    assert(m == TimeSeries.mergeOrderedSeqs(l1, l2))
  }

}
