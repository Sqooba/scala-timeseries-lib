package io.sqooba.oss.timeseries.window

import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue
import scala.concurrent.duration.TimeUnit

/** An aggregator that relies on the passed entries' integral function.
  *
  * Very similar to the SummingAggregator, but takes the validity of each entry into
  * account. It is therefore time-aware and needs entries to be contained in the
  * window.
  *
  * @param timeunit that will be passe to the entries' integral function
  * @param initialValue to initialise the aggregator with.
  */
class IntegralAggregator[T](
    timeunit: TimeUnit,
    initialValue: Double = .0
)(implicit n: Numeric[T])
    extends TimeAwareReversibleAggregator[T, Double] {

  private var integral = initialValue

  def currentValue: Option[Double] = Some(integral)

  def addEntry(entry: TSEntry[T]): Unit =
    integral += entry.integral(timeunit)

  def dropEntry(entry: TSEntry[T]): Unit =
    integral -= entry.integral(timeunit)

}
