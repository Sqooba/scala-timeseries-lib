package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

class TimeSeriesBuilderSpec extends FlatSpec with Matchers {

  def newTsb = new TimeSeriesBuilder[Int]

  "A TimeSeriesBuilder" should "return an empty collection when nothing was added" in {
    newTsb.result() shouldBe Vector()
  }
  it should "correctly trim overlapping entries" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 22, 10)
    b += TSEntry(20, 21, 10)
    b.result() shouldBe Vector(TSEntry(10, 42, 5), TSEntry(15, 22, 5), TSEntry(20, 21, 10))
  }
  it should "correctly append non-overlapping equal entries" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(25, 42, 10)
    b += TSEntry(40, 42, 10)
    b.result() shouldBe Vector(TSEntry(10, 42, 10), TSEntry(25, 42, 10), TSEntry(40, 42, 10))
  }
  it should "correctly extend equal contiguous entries" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b += TSEntry(20, 42, 10)
    b.result() shouldBe Vector(TSEntry(10, 42, 20))

  }
  it should "return nothing after having been cleared and be reusable" in {
    val b = newTsb
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b += TSEntry(20, 42, 10)

    b.clear()
    newTsb.result() shouldBe Vector()
    b += TSEntry(10, 42, 10)
    b += TSEntry(15, 42, 10)
    b += TSEntry(20, 42, 10)
    b.result() shouldBe Vector(TSEntry(10, 42, 20))
  }
  it should "not allow result to be called twice without a clearing" in {
    val b = newTsb
    b.result() shouldBe Vector()
    val ex = intercept[IllegalStateException] {
      b.result()
    }
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

}
