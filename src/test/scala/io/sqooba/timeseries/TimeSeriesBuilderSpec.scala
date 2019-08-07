package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.scalatest.{FlatSpec, Matchers}

class TimeSeriesBuilderSpec extends FlatSpec with Matchers {

  private def newTsb              = new TimeSeriesBuilder[Int]
  private def newTsbNoCompression = new TimeSeriesBuilder[Int](compress = false)

  private val overlappingEntries        = Seq(TSEntry(10, 42, 10), TSEntry(15, 22, 10), TSEntry(20, 21, 10))
  private val nonOverlappingEntries     = Seq(TSEntry(10, 42, 10), TSEntry(25, 42, 10), TSEntry(40, 42, 10))
  private val equalContiguousEntries    = Seq(TSEntry(10, 42, 10), TSEntry(15, 42, 10), TSEntry(20, 42, 10))
  private val notEqualContiguousEntries = Seq(TSEntry(10, -1, 10), TSEntry(15, 2, 10), TSEntry(20, 42, 10))

  "A TimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    newTsb.vectorResult() shouldBe Vector()
  }

  it should "correctly trim overlapping entries" in {
    val b = newTsb
    b ++= overlappingEntries

    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 5), TSEntry(15, 22, 5), TSEntry(20, 21, 10))
  }

  it should "correctly append non-overlapping equal entries" in {
    val b = newTsb
    b ++= nonOverlappingEntries

    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 10), TSEntry(25, 42, 10), TSEntry(40, 42, 10))
  }

  it should "correctly extend equal contiguous entries if compressing" in {
    val b = newTsb
    b ++= equalContiguousEntries

    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 20))
  }

  it should "not merge (only trim) equal contiguous entries if not compressing" in {
    val b = newTsbNoCompression
    b ++= equalContiguousEntries

    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 5), TSEntry(15, 42, 5), TSEntry(20, 42, 10))
  }

  it should "correctly set the isCompressed flag on the result" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)

    val result = b.result()
    result.entries shouldBe Seq(TSEntry(10, 42, 15))
    result.isCompressed shouldBe true

    val bNoComp = newTsbNoCompression
    bNoComp += TSEntry(10, 42, 10)
    bNoComp += TSEntry(15, 42, 10)

    val resultNoComp = bNoComp.result()
    resultNoComp.entries shouldBe Seq(TSEntry(10, 42, 5), TSEntry(15, 42, 10))
    resultNoComp.isCompressed shouldBe false
  }

  it should "return nothing after having been cleared and be reusable" in {
    val b = newTsb
    b ++= equalContiguousEntries

    b.clear()
    b ++= equalContiguousEntries
    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 20))
  }
  it should "not allow result to be called twice without a clearing" in {
    val b = newTsb
    b.vectorResult() shouldBe Vector()
    val ex = intercept[IllegalStateException] {
      b.vectorResult()
    }
  }

  it should "raise an exception if unordered TSEntry are added" in {
    val b = newTsb
    b += TSEntry(5, 0, 1)

    assertThrows[IllegalArgumentException](b += TSEntry(0, 0, 1))
  }

  "currentLastEntry" should "correctly return the current last entry of the builder" in {
    val b = newTsb
    b.definedUntil shouldBe None
    b += TSEntry(10, 42, 10)
    b.definedUntil shouldBe Some(20)
    b += TSEntry(15, 42, 10)
    b.definedUntil shouldBe Some(25)
    b.clear()
    b.definedUntil shouldBe None
  }

  "result" should "return an EmptyTimeSeries if no entry is appended" in {
    val b = newTsb

    b.result() should equal(EmptyTimeSeries)
  }

  it should "be the given TSEntry if there is only one appended" in {
    val entry = TSEntry(1, 1, 1)

    val b = newTsb

    b += entry

    b.result() should equal(entry)
  }

  it should "be a VectorTimeSeries in other cases" in {
    val b = newTsb

    val data = Vector(
      TSEntry(1, 1, 1),
      TSEntry(3, 3, 3)
    )

    b ++= data
    val resultSeries = b.result()

    assert(resultSeries.isInstanceOf[VectorTimeSeries[Int]])
    resultSeries.entries should equal(data)
  }

  it should "set the continuous domain flag for overlapping entries" in {
    val b = newTsb
    b ++= overlappingEntries

    b.result.isDomainContinuous shouldBe true

    val bNoCompress = newTsbNoCompression
    bNoCompress ++= overlappingEntries

    bNoCompress.result.isDomainContinuous shouldBe true
  }

  it should "not set the continuous domain flag for non-overlapping entries" in {
    val b = newTsb
    b ++= nonOverlappingEntries
    b.result().isDomainContinuous shouldBe false
  }

  it should "set the continuous domain flag for contiguous entries" in {
    val bEqual = newTsb
    bEqual ++= equalContiguousEntries
    bEqual.result().isDomainContinuous shouldBe true

    val bNotEqual = newTsb
    bNotEqual ++= notEqualContiguousEntries
    bNotEqual.result().isDomainContinuous shouldBe true
  }
}
