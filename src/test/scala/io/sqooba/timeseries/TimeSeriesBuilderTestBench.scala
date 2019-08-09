package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{EmptyTimeSeries, TSEntry}
import org.scalatest.{FlatSpec, Matchers}

trait TimeSeriesBuilderTestBench extends Matchers { this: FlatSpec =>

  private val overlappingEntries        = Seq(TSEntry(10, 42, 10), TSEntry(15, 22, 10), TSEntry(20, 21, 10))
  private val nonOverlappingEntries     = Seq(TSEntry(10, 42, 10), TSEntry(25, 42, 10), TSEntry(40, 42, 10))
  private val equalContiguousEntries    = Seq(TSEntry(10, 42, 10), TSEntry(15, 42, 10), TSEntry(20, 42, 10))
  private val notEqualContiguousEntries = Seq(TSEntry(10, -1, 10), TSEntry(15, 2, 10), TSEntry(20, 42, 10))

  def aTimeSeriesBuilder(newBuilderFromCompression: Boolean => TimeSeriesBuilder[Int]) {

    def newTimeSeriesBuilder           = newBuilderFromCompression(true)
    def newTimeSeriesBuilderNoCompress = newBuilderFromCompression(false)

    it should "return an empty timeseries if nothing was added" in {
      newTimeSeriesBuilder.result() shouldBe EmptyTimeSeries
      newTimeSeriesBuilderNoCompress.result() shouldBe EmptyTimeSeries
    }

    it should "correctly trim overlapping entries" in {
      val b = newTimeSeriesBuilder
      b ++= overlappingEntries

      b.result().entries shouldBe Seq(TSEntry(10, 42, 5), TSEntry(15, 22, 5), TSEntry(20, 21, 10))
    }

    it should "correctly append non-overlapping equal entries" in {
      val b = newTimeSeriesBuilder
      b ++= nonOverlappingEntries

      b.result().entries shouldBe Seq(TSEntry(10, 42, 10), TSEntry(25, 42, 10), TSEntry(40, 42, 10))
    }

    it should "correctly extend equal contiguous entries if compressing" in {
      val b = newTimeSeriesBuilder
      b ++= equalContiguousEntries

      b.result().entries shouldBe Seq(TSEntry(10, 42, 20))
    }

    it should "not merge (only trim) equal contiguous entries if not compressing" in {
      val b = newTimeSeriesBuilderNoCompress
      b ++= equalContiguousEntries

      b.result().entries shouldBe Seq(TSEntry(10, 42, 5), TSEntry(15, 42, 5), TSEntry(20, 42, 10))
    }

    it should "correctly set the isCompressed flag on the result" in {
      val b = newTimeSeriesBuilder
      b += TSEntry(10, 42, 10)
      b += TSEntry(15, 42, 10)

      val result = b.result()
      result.isCompressed shouldBe true
      result.entries shouldBe Seq(TSEntry(10, 42, 15))

      val bNoComp = newTimeSeriesBuilderNoCompress
      bNoComp += TSEntry(10, 42, 10)
      bNoComp += TSEntry(15, 42, 10)

      val resultNoComp = bNoComp.result()
      resultNoComp.isCompressed shouldBe false
      resultNoComp.entries shouldBe Seq(TSEntry(10, 42, 5), TSEntry(15, 42, 10))
    }

    it should "return nothing after having been cleared and be reusable" in {
      val b = newTimeSeriesBuilder
      b ++= equalContiguousEntries

      b.clear()
      b ++= equalContiguousEntries
      b.result().entries shouldBe Seq(TSEntry(10, 42, 20))
    }

    it should "not allow result to be called twice without a clearing" in {
      val b = newTimeSeriesBuilder
      b.result().entries shouldBe Seq()

      intercept[IllegalStateException] {
        b.result().entries
      }
    }

    it should "raise an exception if unordered TSEntry are added" in {
      val b = newTimeSeriesBuilder
      b += TSEntry(5, 0, 1)

      assertThrows[IllegalArgumentException](b += TSEntry(0, 0, 1))
    }

    it should "correctly return definedUntil based on the last element added" in {
      val b = newTimeSeriesBuilder
      b.definedUntil shouldBe None

      b += TSEntry(10, 42, 10)
      b.definedUntil shouldBe Some(20)

      b += TSEntry(15, 42, 10)
      b.definedUntil shouldBe Some(25)

      b.clear()
      b.definedUntil shouldBe None
    }

    it should "be the given TSEntry if there is only one appended" in {
      val entry = TSEntry(1, 1, 1)
      val b     = newTimeSeriesBuilder

      b += entry
      b.result() should equal(entry)
    }

    it should "set the continuous domain flag for overlapping entries" in {
      val b = newTimeSeriesBuilder
      b ++= overlappingEntries

      b.result.isDomainContinuous shouldBe true

      val bNoCompress = newTimeSeriesBuilderNoCompress
      bNoCompress ++= overlappingEntries

      bNoCompress.result.isDomainContinuous shouldBe true
    }

    it should "not set the continuous domain flag for non-overlapping entries" in {
      val b = newTimeSeriesBuilder
      b ++= nonOverlappingEntries
      b.result().isDomainContinuous shouldBe false
    }

    it should "set the continuous domain flag for contiguous entries" in {
      val bEqual = newTimeSeriesBuilder
      bEqual ++= equalContiguousEntries
      bEqual.result().isDomainContinuous shouldBe true

      val bNotEqual = newTimeSeriesBuilder
      bNotEqual ++= notEqualContiguousEntries
      bNotEqual.result().isDomainContinuous shouldBe true
    }
  }
}
