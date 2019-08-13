package io.sqooba.public.timeseries.windowing

import io.sqooba.public.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue

/**
  * Tooling to efficiently compute aggregates over sliding windows,
  * in contexts where this is possible.
  *
  * Assuming we want to aggregate the content of a window, and to do so for each
  * different window returned by a WindowSlider, many iterations will be required.
  *
  * Depending on the aggregation function, this is however not required:
  *
  * For simple cases like addition or multiplication and any situation
  * where the contributions of a single entry to the aggregated value may
  * be reversed, we can compute an aggregated value for each window in
  * linear time.
  *
  * The reversible aggregator will be applied sequentially, so it may
  * keep track of any state from one addition or removal to the next.
  *
  * @tparam T the type of the entries being aggregated over
  * @tparam A the type of the aggregated value
  */
trait ReversibleAggregator[T, A] {

  /**
    * @return the current aggregated value
    */
  def currentValue: Option[A]

  /**
    * Update the internal aggregated value according to the entry that is about
    * to be added to the window.
    *
    * @param e the entry that is about to enter the window
    * @param currentWindow the current content of the window: it does not
    *                      include 'e' at this point.
    */
  // TODO: consider returning the resulting aggregated value?
  def addEntry(e: TSEntry[T], currentWindow: Queue[TSEntry[T]]): Unit

  /**
    * Updates the aggregated value according to the fact that
    * the head of the currentWindow is being removed.
    *
    * @param currentWindow the current content of the window. It still
    *                      contains the entry that has to be removed
    */
  // TODO: consider returning the resulting aggregated value?
  def dropHead(currentWindow: Queue[TSEntry[T]]): Unit

  /**
    * Combine the addition and the removal of entries from the aggregated value.
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

}

object DoNothingAggregator extends ReversibleAggregator[Nothing, Nothing] {

  def currentValue: Option[Nothing] = None

  def addEntry(e: TSEntry[Nothing], currentWindow: Queue[TSEntry[Nothing]]): Unit = Unit

  def dropHead(currentWindow: Queue[TSEntry[Nothing]]): Unit = Unit

  override def addAndDrop(add: TSEntry[Nothing], currentWindow: Queue[TSEntry[Nothing]]): Unit = Unit

}
