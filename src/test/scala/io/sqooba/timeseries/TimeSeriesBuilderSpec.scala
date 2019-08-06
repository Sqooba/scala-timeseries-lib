package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry, VectorTimeSeries}
import org.scalatest.{FlatSpec, Matchers}

class TimeSeriesBuilderSpec extends FlatSpec with Matchers {

  private def newTsb              = new TimeSeriesBuilder[Int]
  private def newTsbNoCompression = new TimeSeriesBuilder[Int](compress = false)

  "A TimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    newTsb.vectorResult() shouldBe Vector()
  }

  it should "correctly trim overlapping entries" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 22, 10)
    b += TSEntry(20, 21, 10)
    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 5), TSEntry(15, 22, 5), TSEntry(20, 21, 10))
  }
  it should "correctly append non-overlapping equal entries" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(25, 42, 10)
    b += TSEntry(40, 42, 10)
    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 10), TSEntry(25, 42, 10), TSEntry(40, 42, 10))
  }
  it should "correctly extend equal contiguous entries if compressing" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b += TSEntry(20, 42, 10)
    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 20))
  }

  it should "not merge (only trim) equal contiguous entries if not compressing" in {
    val b = newTsbNoCompression
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b += TSEntry(20, 42, 10)
    b.vectorResult() shouldBe Vector(TSEntry(10, 42, 5), TSEntry(15, 42, 5), TSEntry(20, 42, 10))
  }

  it should "correctly set the isCompressed flag on the result" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b.result() shouldBe TimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(10, 42, 15)), isCompressed = true)

    val bNoComp = newTsbNoCompression
    bNoComp += TSEntry(10, 42, 10)
    bNoComp += TSEntry(15, 42, 10)
    bNoComp.result() shouldBe
      TimeSeries.ofOrderedEntriesUnsafe(Seq(TSEntry(10, 42, 5), TSEntry(15, 42, 10)), isCompressed = false)
  }

  it should "return nothing after having been cleared and be reusable" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b += TSEntry(20, 42, 10)

    b.clear()
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b += TSEntry(20, 42, 10)
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

    val VectorTimeSeries(generatedData, _) = b.result()

    generatedData should equal(data)
  }

}
