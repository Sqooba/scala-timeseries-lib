package io.sqooba.timeseries.windowing

import io.sqooba.timeseries.immutable.TSEntry
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Queue

class SummingAggregatorSpec extends FlatSpec with Matchers {

  "A SummingAggregator" should "be initialized at 0" in {
    new SummingAggregator[Double]().currentValue shouldBe Some(.0)
  }
  it should "correctly increment the sum" in {
    val sa = new SummingAggregator[Double]()
    sa.addEntry(TSEntry(0, 42.0, 1), Queue())
    sa.currentValue shouldBe Some(42.0)

    sa.addEntry(TSEntry(0, 42.0, 10), Queue())
    sa.currentValue shouldBe Some(84.0)
  }
  it should "do nothing with the existing window when adding a value" in {
    // Just checks we don't run into a NPE...
    noException shouldBe thrownBy(new SummingAggregator[Double]().addEntry(TSEntry(0, 42.0, 1), null))
  }

  it should "correctly decrement the sum using the first entry in the passed queue" in {
    val sa = new SummingAggregator[Double]()
    sa.dropHead(Queue(TSEntry(0, 42.0, 1)))

    sa.currentValue shouldBe Some(-42.0)

    sa.dropHead(Queue(TSEntry(0, 42.0, 10), TSEntry(10, 13, 1)))

    sa.currentValue shouldBe Some(-84.0)
  }
}
