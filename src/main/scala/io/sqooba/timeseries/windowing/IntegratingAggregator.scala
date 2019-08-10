package io.sqooba.timeseries.windowing

import io.sqooba.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue
import scala.concurrent.duration.TimeUnit

/**
  * An aggregator that relies on the passed entries' integral function.
  *
  * Very similar to the SummingAggregator, but takes the validity of each
  * entry into account.
  *
  * @param timeunit that will be passe to the entries' integral function
  * @param initialValue to initialise the aggregator with.
  */
class IntegratingAggregator[T](timeunit: TimeUnit, initialValue: Double = .0)(implicit n: Numeric[T]) extends ReversibleAggregator[T, Double] {

  private var integral = initialValue

  def currentValue: Option[Double] = Some(integral)

  def addEntry(e: TSEntry[T], currentWindow: Queue[TSEntry[T]]): Unit =
    integral += e.integral(timeunit)

  def dropHead(currentWindow: Queue[TSEntry[T]]): Unit =
    integral -= currentWindow.head.integral(timeunit)

}
