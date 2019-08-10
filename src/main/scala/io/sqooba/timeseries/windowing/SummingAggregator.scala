package io.sqooba.timeseries.windowing

import io.sqooba.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue

/**
  * A reversible aggregator that keeps track of the total sum of the values
  * present in each entry that is at least partially within the window's domain.
  *
  * Discontinuities in the domain of definition between entries are completely ignored.
  */
class SummingAggregator[T](implicit n: Numeric[T]) extends ReversibleAggregator[T, T] {

  import n._

  private var sum = n.zero

  def currentValue: Option[T] = Some(sum)

  def addEntry(e: TSEntry[T], currentWindow: Queue[TSEntry[T]]): Unit =
    sum += e.value

  def dropHead(currentWindow: Queue[TSEntry[T]]): Unit =
    sum -= currentWindow.head.value

}
