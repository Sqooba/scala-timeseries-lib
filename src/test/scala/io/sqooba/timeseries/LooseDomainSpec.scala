package io.sqooba.timeseries

import io.sqooba.basespec.SqoobaSpec
import io.sqooba.timeseries.immutable.{EmptyTimeSeries, LooseDomain, TSEntry, VectorTimeSeries}

class LooseDomainSpec extends SqoobaSpec {

  private def assertDomain(looseDomain: LooseDomain, start: Long, until: Long) = {
    // Check internal bounds
    assert(looseDomain contains start)
    assert(looseDomain.contains(until - 1))

    // Check external bounds
    assert(!looseDomain.contains(start - 1))
    assert(!looseDomain.contains(until))
  }

  private val (start, end, validity) = (0, 10, 3)

  private val niceAndLongTimeSeries: VectorTimeSeries[None.type] = {
    val smallestEntry = TSEntry(start, None, validity)
    val biggestEntry  = TSEntry(end, None, validity)

    val builder = new TimeSeriesBuilder[None.type]()

    builder += smallestEntry
    builder += TSEntry(2, None, 1)
    builder += TSEntry(9, None, 2)
    builder += biggestEntry

    new VectorTimeSeries(builder.result())
  }

  private val timeseriesSeq: Seq[TimeSeries[None.type]] = List[TimeSeries[None.type]](
    niceAndLongTimeSeries,
    EmptyTimeSeries(),
    TSEntry(3, None, 2)
  )

  "The loose domain" should "be none for empty TimeSeries" in {
    EmptyTimeSeries[Any]().looseDomain should equal(None)
  }

  it should "be trivial for single entry" in {
    val entry = TSEntry[Any](10, None, 2)

    val Some(looseDomain) = entry.looseDomain

    val untilDomain = entry.timestamp + entry.validity

    assertDomain(looseDomain, entry.timestamp, untilDomain)
  }

  it should "contains the bounds of a multi-value TimeSeries" in {
    val ts                = niceAndLongTimeSeries
    val Some(looseDomain) = ts.looseDomain

    val untilDomain = end + validity

    assertDomain(looseDomain, start, untilDomain)
  }

  "The union of loose domains" should "work with any subtype of TimeSeries" in {
    val Some(looseDomain) = TimeSeries.unionLooseDomains(timeseriesSeq)

    assertDomain(looseDomain, start, end + validity)
  }

  it should "be None if an empty Seq is given" in {
    TimeSeries.unionLooseDomains(Seq.empty) should equal(None)
  }

  it should "be None if a Seq of empty is given" in {
    TimeSeries.unionLooseDomains(List(EmptyTimeSeries(), EmptyTimeSeries())) should equal(None)
  }

  "The intersection of loose domains" should "not exists if there is at least one EmptyTimeSeries" in {
    val looseDomainOpt = TimeSeries.intersectLooseDomains(timeseriesSeq)

    looseDomainOpt should be(None)
  }

  it should "consider the smaller case if there is no empty time series" in {
    val tss = timeseriesSeq filter (_.size() != 0)

    val Some(looseDomain) = TimeSeries.intersectLooseDomains(tss)

    assertDomain(looseDomain, 3, 5)
  }

  it should "be None if empty Seq" in {
    TimeSeries.intersectLooseDomains(Seq.empty) should equal(None)
  }

  it should "be None if the two LooseDomains are not overlapping" in {
    val xs = List(
      TSEntry(0, None, 3),
      TSEntry(10, None, 1)
    )

    TimeSeries.intersectLooseDomains(xs) should equal(None)
  }

}
