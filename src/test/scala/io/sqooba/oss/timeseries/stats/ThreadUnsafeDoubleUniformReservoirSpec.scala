package io.sqooba.oss.timeseries.stats

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class ThreadUnsafeDoubleUniformReservoirSpec extends FlatSpec with Matchers {

  "A ThreadUnsafeDoubleUniformReservoir" should "report its size as at most the size of the underlying array" in {
    val res = new ThreadUnsafeDoubleUniformReservoir(2)

    res.size shouldBe 0
    res.update(1.0)
    res.size shouldBe 1
    res.update(2.0)
    res.size shouldBe 2
    res.update(3.0)
    res.size shouldBe 2
  }
  it should "properly handle adding many more values than the reservoir's size" in {
    val res  = new ThreadUnsafeDoubleUniformReservoir(10)
    val rand = new Random()
    noException should be thrownBy {
      for (_ <- 0 to 1000) {
        res.update(rand.nextDouble())
      }
      res.snapshot()
    }
  }
  it should "properly add all values to the reservoir as long as it is not filled" in {
    val res = new ThreadUnsafeDoubleUniformReservoir(2)

    res.update(1.0)
    res.update(2.0)
    res.values shouldBe Array(1.0, 2.0)
  }
  it should "properly pass the real min and max to the snapshot" in {
    new ThreadUnsafeDoubleUniformReservoir(2)
      .update(1.0)
      .update(2.0)
      .update(3.0)
      .snapshot()
      .shouldBe(Stats(1.0, 3.0, 2.0, 1.4142135623730951, 2.0))
  }
  it should "return a zero-snapshot if no value was sampled" in {
    new ThreadUnsafeDoubleUniformReservoir(2)
      .snapshot()
      .shouldBe(Stats(.0, .0, .0, .0, .0))
  }
  it should "properly copy the reservoir's relevant entries if it was not filled" in {
    val res = new ThreadUnsafeDoubleUniformReservoir(3)
    res
      .update(2.0)
      .update(1.0)

    res.snapshot() shouldBe Stats(1.0, 2.0, 1.5, 0.7071067811865476, 1.5)
    res.values shouldBe Array(2.0, 1.0, .0)

    // Fill up the reservoir
    res.update(3.0)
    res.values shouldBe Array(2.0, 1.0, 3.0)
    // Now the snapshot function passes the whole reservoir to underlying logic
    res.snapshot() shouldBe Stats(1.0, 3.0, 2.0, 1.0, 2.0)
    res.values shouldBe Array(1.0, 2.0, 3.0)
  }
}
