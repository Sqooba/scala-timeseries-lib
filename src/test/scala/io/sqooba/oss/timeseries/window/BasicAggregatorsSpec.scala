package io.sqooba.oss.timeseries.window

import io.sqooba.oss.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Queue

class BasicAggregatorsSpec extends FlatSpec with Matchers {

  "SumAggregator" should "be initialized at 0" in {
    Aggregator.sum[Double].currentValue shouldBe Some(.0)
  }

  it should "correctly increment the sum" in {
    val sa = Aggregator.sum[Double]
    sa.addEntry(TSEntry(0, 42.0, 1))
    sa.currentValue shouldBe Some(42.0)

    sa.addEntry(TSEntry(0, 42.0, 10))
    sa.currentValue shouldBe Some(84.0)
  }

  it should "do nothing with the existing window when adding a value" in {
    // Just checks we don't run into a NPE...
    noException shouldBe thrownBy(
      new SumAggregator[Double]().addEntry(TSEntry(0, 42.0, 1))
    )
  }

  it should "correctly decrement the sum using the first entry in the passed queue" in {
    val sa = new SumAggregator[Double]()
    sa.dropEntry(TSEntry(0, 42.0, 1))

    sa.currentValue shouldBe Some(-42.0)

    sa.dropHead(Queue(TSEntry(0, 42.0, 10), TSEntry(10, 13, 1)))

    sa.currentValue shouldBe Some(-84.0)
  }

  "MeanAggregator" should "return none if no entry was given" in {
    new MeanAggregator[Double]().currentValue shouldBe None
  }

  it should "correctly calculate the mean of the given entries" in {
    val agg = Aggregator.mean[Int]

    agg.addEntry(TSEntry(0, 20, 5))
    agg.addEntry(TSEntry(5, -10, 10))
    agg.addEntry(TSEntry(15, 7, 25))

    agg.currentValue should contain(4.375)

    agg.dropEntry(TSEntry(0, 20, 5))
    agg.currentValue should contain((-10 * 10 + 7 * 25) / 35.0)

    agg.dropHead(Queue(TSEntry(5, -10, 10), TSEntry(15, 7, 25)))
    agg.currentValue shouldBe Some(7)

    agg.dropHead(Queue(TSEntry(15, 7, 25)))
    agg.currentValue shouldBe None
  }

  "StdAggregator" should "return none if no entry was given" in {
    new StdAggregator[Double]().currentValue shouldBe None
  }

  it should "correctly calculate the biased standard deviation of the given entries" in {
    val agg = Aggregator.std[Int]

    agg.addEntry(TSEntry(0, 20, 5))
    agg.currentValue shouldBe Some(0)

    agg.addEntry(TSEntry(5, -10, 10))
    agg.currentValue.isDefined shouldBe true
    agg.currentValue.get shouldBe 14.142 +- 0.001

    agg.addEntry(TSEntry(15, 7, 25))
    agg.currentValue.isDefined shouldBe true
    agg.currentValue.get shouldBe 9.299 +- 0.001

    agg.dropEntry(TSEntry(0, 20, 5))
    agg.currentValue.isDefined shouldBe true
    agg.currentValue.get shouldBe 7.679 +- 0.001

    agg.dropHead(Queue(TSEntry(5, -10, 10), TSEntry(15, 7, 25)))
    agg.currentValue shouldBe Some(0)

    agg.dropHead(Queue(TSEntry(15, 7, 25)))
    agg.currentValue shouldBe None
  }

  "Aggregator.min" should "always return the minimum" in {
    val agg = Aggregator.min[Float]

    agg.addEntry(TSEntry(0, 20, 5))
    agg.currentValue shouldBe Some(20)

    agg.addEntry(TSEntry(5, -10, 10))
    agg.currentValue shouldBe Some(-10)

    agg.addEntry(TSEntry(15, 7, 25))
    agg.currentValue shouldBe Some(-10)

    agg.dropEntry(TSEntry(0, 20, 5))
    agg.currentValue shouldBe Some(-10)

    agg.dropHead(Queue(TSEntry(5, -10, 10), TSEntry(15, 7, 25)))
    agg.currentValue shouldBe Some(7)

    agg.dropEntry(TSEntry(15, 7, 25))
    agg.currentValue shouldBe None
  }

  "Aggregator.max" should "always return the maximum" in {
    val agg = Aggregator.max[Float]

    agg.addEntry(TSEntry(0, 20, 5))
    agg.currentValue shouldBe Some(20)

    agg.addEntry(TSEntry(5, -10, 10))
    agg.currentValue shouldBe Some(20)

    agg.addEntry(TSEntry(15, 7, 25))
    agg.currentValue shouldBe Some(20)

    agg.dropEntry(TSEntry(0, 20, 5))
    agg.currentValue shouldBe Some(7)

    agg.addEntry(TSEntry(40, 5, 10))
    agg.currentValue shouldBe Some(7)

    agg.addEntry(TSEntry(50, 1000, 10))
    agg.currentValue shouldBe Some(1000)

    agg.dropEntry(TSEntry(5, -10, 10))
    agg.currentValue shouldBe Some(1000)

    agg.dropEntry(TSEntry(15, 7, 25))
    agg.currentValue shouldBe Some(1000)

    agg.dropEntry(TSEntry(40, 5, 10))
    agg.currentValue shouldBe Some(1000)

    agg.addEntry(TSEntry(60, -5, 10))
    agg.currentValue shouldBe Some(1000)

    agg.dropEntry(TSEntry(50, 1000, 10))
    agg.currentValue shouldBe Some(-5)

    agg.dropEntry(TSEntry(60, -5, 10))
    agg.currentValue shouldBe None
  }

}
