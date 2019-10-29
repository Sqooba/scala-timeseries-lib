package io.sqooba.oss.timeseries.window

import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.mutable

/** An aggregator that does strictly nothing. Used for window creation without aggregation. */
object DoNothingAggregator extends TimeUnawareReversibleAggregator[Nothing, Nothing] {

  def currentValue: Option[Nothing] = None

  def addEntry(entry: TSEntry[Nothing]): Unit = ()

  def dropEntry(entry: TSEntry[Nothing]): Unit = ()
}

/** A reversible aggregator that keeps track of the total sum of the values
  * present in each entry that is at least partially within the window's domain.
  *
  * Discontinuities in the domain of definition between entries are completely ignored.
  */
class SumAggregator[T](implicit n: Numeric[T]) extends TimeUnawareReversibleAggregator[T, T] {
  import n._

  private var sum = n.zero

  def currentValue: Option[T] = Some(sum)

  def addEntry(entry: TSEntry[T]): Unit =
    sum += entry.value

  def dropEntry(entry: TSEntry[T]): Unit =
    sum -= entry.value

}

/** Reversible aggregator that calculates the mean of the values in the window
  * weighted by time of validity. It is therefore time-aware and needs entries
  * to be contained in the window.
  */
class MeanAggregator[T](implicit n: Numeric[T]) extends TimeAwareReversibleAggregator[T, Double] {
  import n._

  // Sum of X_i * d_i
  private var sum = .0
  // Sum of d_i
  private var durations: Long = 0

  def currentValue: Option[Double] =
    // Sum of X_i * d_i / Sum of d_i or None
    if (durations > 0) Some(sum / durations) else None

  def addEntry(entry: TSEntry[T]): Unit = {
    sum += weighted(entry)
    durations += entry.validity
  }

  def dropEntry(entry: TSEntry[T]): Unit = {
    sum -= weighted(entry)
    durations -= entry.validity
  }

  private def weighted(e: TSEntry[T]): Double = e.value.toDouble * e.validity
}

/** Reversible aggregator that calculates the (biased) standard deviation (e.g.
  * square root of the variance) of the values in the window, weighted by time of
  * validity. It is therefore time-aware and needs entries to be contained in the
  * window.
  */
class StdAggregator[T](implicit n: Numeric[T]) extends TimeAwareReversibleAggregator[T, Double] {
  import n._

  // Weighted mean of squares E_w[X^2]
  private val squareMean = new MeanAggregator[T]()
  // Weighted mean E_w[X]
  private val mean = new MeanAggregator[T]()

  def currentValue: Option[Double] =
    for {
      mean    <- mean.currentValue
      squares <- squareMean.currentValue
    } yield
    // std = sqrt{ E_w[X^2] - E_w[X]^2 }
    Math.sqrt(squares - mean * mean)

  def addEntry(entry: TSEntry[T]): Unit = {
    squareMean.addEntry(entry.map(v => v * v))
    mean.addEntry(entry)
  }

  def dropEntry(entry: TSEntry[T]): Unit = {
    squareMean.dropEntry(entry.map(v => v * v))
    mean.dropEntry(entry)
  }
}

/** A reversible aggregator that keeps track of the minimum of the values
  * present in the window. You can get a maximum aggregator by simply
  * reversing the ordering passed as an implicit.
  *
  * The aggregator uses an ordered internal queue and discards values that
  * can never be the minimum.
  */
class MinAggregator[T](implicit n: Ordering[T]) extends TimeUnawareReversibleAggregator[T, T] {
  import n._

  private var minQueue = mutable.Queue[T]()

  override def currentValue: Option[T] = minQueue.headOption

  def addEntry(e: TSEntry[T]): Unit = {
    // In Scala 2.13, this can be more elegantly solved:
    //  minQueue.takeWhileInPlace(_ <= e.value).append(e.value)
    minQueue = minQueue.takeWhile(_ <= e.value)
    minQueue.enqueue(e.value)
  }

  def dropEntry(entry: TSEntry[T]): Unit = {
    if (minQueue.head == entry.value) minQueue.dequeue()
  }
}

/** Template aggregator that keeps track of the entire window. It is therefore not
  * efficient for most calculations.
  */
abstract class QueueAggregator[T, A] extends TimeUnawareReversibleAggregator[T, A] {

  private[window] val queue: mutable.Queue[T] = mutable.Queue.empty

  def addEntry(e: TSEntry[T]): Unit =
    queue += e.value

  def dropEntry(entry: TSEntry[T]): Unit =
    queue.dequeue
}
