package io.sqooba.oss.timeseries.window

import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue

/** Extension to the Aggregator that also supports removing entries from the
  * aggregated value. Assuming we want to aggregate the content of a window, and to
  * do so for each different window returned by WindowSlider, many iterations will
  * be required.
  *
  * Depending on the aggregation function, this is however not required: For simple
  * cases like addition or multiplication and any situation where the contributions
  * of a single entry to the aggregated value may be reversed, we can compute an
  * aggregated value for each window in linear time.
  *
  * The reversible aggregator will be applied sequentially, so it may keep track of
  * any state from one addition or removal to the next.
  *
  * Some aggregations depend on the duration of the entries like integration or
  * averaging, others like min max don't. To keep those types of aggregations well
  * separated, implementations need to extend either the time-aware or the
  * time-unaware subtrait. This allows us to use different windowing functions for
  * the two types.
  *
  * @tparam T the type of the entries being aggregated over
  * @tparam A the type of the aggregated value
  */
sealed trait ReversibleAggregator[T, A] extends Aggregator[T, A] {

  /** Updates the aggregated value according to the fact that
    * the head of the currentWindow is being removed.
    *
    * @param currentWindow the current content of the window. It still
    *                      contains the entry that has to be removed
    */
  // TODO: consider returning the resulting aggregated value?
  def dropHead(currentWindow: Queue[TSEntry[T]]): Unit =
    dropEntry(currentWindow.head)

  /** Updates the aggregated value according to the fact that
    * this entry is being removed.
    *
    * @param entry to remove from the head of the window
    */
  // TODO: consider returning the resulting aggregated value?
  def dropEntry(entry: TSEntry[T]): Unit

  /** Combine the addition and the removal of entries from the aggregated value.
    *
    * @param add the value that will be added
    * @param currentWindow the current window, from which we will drop the first entry.
    *                      Note that it does not yet contain 'add'
    */
  def addAndDrop(add: TSEntry[T], currentWindow: Queue[TSEntry[T]]): Unit = {
    dropHead(currentWindow)
    // addEntry needs to work on the updated window
    addEntry(add, currentWindow.tail)
  }

  /** Combine the addition and the removal of entries from the aggregated value.
    *
    * @param add the entry that will be added at the tail
    * @param remove the entry that will be removed at the head
    */
  def addAndDrop(add: TSEntry[T], remove: TSEntry[T]): Unit = {
    dropEntry(remove)
    addEntry(add)
  }
}

/** This trait should be extended by all aggregators that depend on the time/duration
  * in their calculation like integration, averaging over time etc.
  */
trait TimeAwareReversibleAggregator[T, A] extends ReversibleAggregator[T, A]

/** This trait should be extended by all aggregators that don't depend on the
  * duration in their calculation like min, max, median.
  */
trait TimeUnawareReversibleAggregator[T, A] extends ReversibleAggregator[T, A]
