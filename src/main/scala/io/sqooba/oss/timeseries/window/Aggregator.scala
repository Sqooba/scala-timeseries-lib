package io.sqooba.oss.timeseries.window

import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue
import scala.concurrent.duration.TimeUnit

/** Tooling and state wrapper to efficiently compute aggregates over sliding windows,
  * in contexts where this is possible. Instead of aggregating huge slices of a time
  * series, this class iteratively calculates the aggregated value when an entry is
  * added. This allows to only store the summed value instead of all the entries in
  * the case of addition, for example.
  *
  * The aggregator will be applied sequentially, so it may keep track of any state
  * from one entry the next.
  *
  * @tparam T the type of the entries being aggregated over
  * @tparam A the type of the aggregated value
  */
trait Aggregator[T, A] {

  /** @return the current aggregated value or None */
  def currentValue: Option[A]

  /** Update the internal aggregated value according to the entry that is about
    * to be added to the window.
    *
    * @note By default this ignores the currentWindow and passes the entry to
    *       the function that only takes the entry. If you want to use the
    *       entire window in the aggregaton you can override this method.
    *
    * @param e             the entry that is about to enter the window
    * @param currentWindow the current content of the window: it does not
    *                      include 'e' at this point.
    */
  // TODO: consider returning the resulting aggregated value?
  def addEntry(e: TSEntry[T], currentWindow: Queue[TSEntry[T]]): Unit =
    addEntry(e)

  /** Update the internal aggregated value according to the entry that is about
    * to be added to the window.
    *
    * @param e             the entry that is about to enter the window
    */
  // TODO: consider returning the resulting aggregated value?
  def addEntry(e: TSEntry[T]): Unit
}

object Aggregator {

  /** Factory for aggregators that need to act on the entire window, like median.
    *
    * @note Aggregating this way is a lot less efficient for computations that only
    * need little intermediary state. Rather define your own Aggregator for those
    * cases (see for example [[SumAggregator]], [[MinAggregator]].
    *
    * @param f aggregation function from a sequence of entries to an option
    * @return a reversible aggregator
    */
  def queueAggregator[T, A](f: Seq[T] => Option[A]): QueueAggregator[T, A] =
    new QueueAggregator[T, A] {
      override def currentValue: Option[A] = f(queue.toSeq)
    }

  /** See [[SumAggregator]] */
  def sum[T: Numeric]: SumAggregator[T] = new SumAggregator[T]()

  /** See [[MeanAggregator]] */
  def mean[T: Numeric]: MeanAggregator[T] = new MeanAggregator[T]()

  /** Aggregator that returns the minimum of all values in the window.
    * See [[MinAggregator]].
    */
  def min[T: Ordering]: TimeUnawareReversibleAggregator[T, T] = new MinAggregator[T]()

  /** Aggregator that returns the maximum of all values in the window.
    * See [[MinAggregator]].
    */
  def max[T](implicit ordering: Ordering[T]): TimeUnawareReversibleAggregator[T, T] =
    new MinAggregator[T]()(ordering.reverse)

  /** See [[StdAggregator]] */
  def std[T: Numeric]: StdAggregator[T] = new StdAggregator[T]()

  /** See [[IntegralAggregator]] */
  def integral[T: Numeric](timeunit: TimeUnit, initialValue: Double = .0): IntegralAggregator[T] =
    new IntegralAggregator[T](timeunit, initialValue)
}
