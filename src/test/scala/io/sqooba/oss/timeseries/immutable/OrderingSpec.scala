package io.sqooba.oss.timeseries.immutable

import org.scalatest.{FlatSpec, Matchers}

/**
  * Test implicit ordering definition and the (hopefully) efficient
  * merging of ordered collections of time series entries.
  */
class OrderingSpec extends FlatSpec with Matchers {

  "TSEntryOrdering" should "implicitly order TSEntries by timestamp" in {
    val unOrd = Seq(TSEntry(11, "B", 10), TSEntry(1, "A", 10))

    Seq(TSEntry(1, "A", 10), TSEntry(11, "B", 10)) should not be unOrd
    Seq(TSEntry(1, "A", 10), TSEntry(11, "B", 10)) shouldBe unOrd.sorted
  }
}
