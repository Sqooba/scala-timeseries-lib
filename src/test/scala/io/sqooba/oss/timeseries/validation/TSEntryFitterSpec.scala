package io.sqooba.oss.timeseries.validation

import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

class TSEntryFitterSpec extends FlatSpec with Matchers {

  private def newFitter(compress: Boolean) = new TSEntryFitter[Int](compress)

  "A TSEntry" should "return no last entry if nothing was added" in {
    newFitter(true).lastEntry shouldBe None
  }

  it should "compress two overlapping equal entries" in {
    val fitter = newFitter(true)

    fitter.addAndFitLast(TSEntry(1, 77, 10)) shouldBe None
    fitter.addAndFitLast(TSEntry(5, 77, 10)) shouldBe None
    fitter.lastEntry shouldBe Some(TSEntry(1, 77, 14))
  }

  it should "compress two contiguous equal entries" in {
    val fitter = newFitter(true)

    fitter.addAndFitLast(TSEntry(1, 77, 4)) shouldBe None
    fitter.addAndFitLast(TSEntry(5, 77, 5)) shouldBe None
    fitter.lastEntry shouldBe Some(TSEntry(1, 77, 9))
  }

  it should "not compress two equal entries with a gap" in {
    val fitter = newFitter(true)
    val entry1 = TSEntry(1, 77, 5)
    val entry2 = TSEntry(10, 77, 5)

    fitter.addAndFitLast(entry1) shouldBe None
    fitter.addAndFitLast(entry2) shouldBe Some(entry1)
    fitter.lastEntry shouldBe Some(entry2)
  }

  it should "not compress two equal overlapping entries if not compressing" in {
    val fitter = newFitter(false)
    val entry1 = TSEntry(1, 77, 5)
    val entry2 = TSEntry(10, 77, 5)

    fitter.addAndFitLast(entry1) shouldBe None
    fitter.addAndFitLast(entry2) shouldBe Some(entry1)
    fitter.lastEntry shouldBe Some(entry2)
  }

  it should "correctly determine if the domain is contiguous for entries with a gap" in {
    val fitter = newFitter(true)
    val entry1 = TSEntry(1, 77, 5)
    val entry2 = TSEntry(10, 77, 5)

    fitter.addAndFitLast(entry1) shouldBe None
    fitter.isDomainContinuous shouldBe true
    fitter.addAndFitLast(entry2)
    fitter.isDomainContinuous shouldBe false
  }

  it should "correctly determine if the domain is contiguous for overlapping entries" in {
    val fitter = newFitter(false)
    val entry1 = TSEntry(1, 77, 5)
    val entry2 = TSEntry(2, 77, 5)

    fitter.addAndFitLast(entry1) shouldBe None
    fitter.isDomainContinuous shouldBe true
    fitter.addAndFitLast(entry2)
    fitter.isDomainContinuous shouldBe true
  }

  it should "correctly determine if the domain is contiguous for contiguous entries" in {
    val fitter = newFitter(true)
    val entry1 = TSEntry(1, 77, 4)
    val entry2 = TSEntry(5, 77, 5)

    fitter.addAndFitLast(entry1) shouldBe None
    fitter.isDomainContinuous shouldBe true
    fitter.addAndFitLast(entry2)
    fitter.isDomainContinuous shouldBe true
  }

  it should "be reuseable after a clear" in {
    val fitter = newFitter(true)
    val entry1 = TSEntry(1, 77, 4)
    val entry2 = TSEntry(5, 77, 5)

    fitter.addAndFitLast(entry1)
    fitter.lastEntry shouldBe Some(entry1)
    fitter.addAndFitLast(entry2) shouldBe None
    fitter.lastEntry shouldBe Some(TSEntry(1, 77, 9))

    fitter.clear()
    fitter.lastEntry shouldBe None
    fitter.addAndFitLast(entry2) shouldBe None
    fitter.lastEntry shouldBe Some(entry2)
  }

  it should "throw if entries are added in non-chronological order" in {
    val fitter = newFitter(true)
    val entry1 = TSEntry(1, 77, 4)
    val entry2 = TSEntry(5, 77, 5)

    fitter.addAndFitLast(entry2)
    an[IllegalArgumentException] should be thrownBy fitter.addAndFitLast(entry1)
  }
}
