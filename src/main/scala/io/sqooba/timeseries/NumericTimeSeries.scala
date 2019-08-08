package io.sqooba.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.timeseries.immutable.TSEntry

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, Builder}
import scala.concurrent.duration.TimeUnit

object NumericTimeSeries {

  /**
    * Non strict 'plus' operator: wherever one of the time series is undefined,
    * its entry is considered as 0.
    */
  def nonStrictPlus[T, U >: T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[U]): Option[U] = {
    import n._
    (lhO, rhO) match {
      case (Some(l), Some(r)) => Some(l + r)
      case (Some(l), _)       => Some(l)
      case (_, Some(r))       => Some(r)
      case _                  => None
    }
  }

  /**
    * Non strict 'minus' operator: wherever one of the timeseries is undefined it falls back to the given default value.
    * If both defaults are None, the operator is equivalent to #strictMinus.
    *
    * @param lhO the optional left hand value
    * @param rhO the optional right hand value
    * @param lhDefault the optional default for the left hand value
    * @param rhDefault the optional default for the right hand value
    */
  def nonStrictMinus[T](lhDefault: Option[T], rhDefault: Option[T])(lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T]): Option[T] = {
    import n._
    (lhO, rhO) match {
      case (Some(l), Some(r)) => Some(l - r)
      case (Some(l), _)       => rhDefault.map(l - _)
      case (_, Some(r))       => lhDefault.map(_ - r)
      case _                  => rhDefault.flatMap(rDefault => lhDefault.map(_ - rDefault))
    }
  }

  def rolling[T](ts: TimeSeries[T], aggregator: Seq[T] => T, windowMs: Long, compress: Boolean = true)(implicit n: Numeric[T]): TimeSeries[T] =
    ts.mapWithTime(
      { (time, currentVal) =>
        // values from the last `windowMs` milliseconds plus the current val
        aggregator(
          ts.slice(time - windowMs, time).entries.map(_.value) :+ currentVal
        )
      },
      compress
    )

  /**
    * Compute an integral of the passed entries, such that each entry is equal
    * to its own value plus the sum of the entries that came before.
    *
    *
    * Please note that the result is still a step function.
    */
  def stepIntegral[T](seq: Seq[TSEntry[T]], timeUnit: TimeUnit = TimeUnit.MILLISECONDS)(implicit n: Numeric[T]): Seq[TSEntry[Double]] =
    if (seq.isEmpty) {
      Seq()
    } else {
      integrateMe[T](.0, seq, new ArrayBuffer[TSEntry[Double]](seq.size))(timeUnit)(n)
    }

  @tailrec
  private def integrateMe[T](
      sumUntilNow: Double,
      seq: Seq[TSEntry[T]],
      acc: Builder[TSEntry[Double], Seq[TSEntry[Double]]]
  )(timeUnit: TimeUnit)(implicit n: Numeric[T]): Seq[TSEntry[Double]] = {
    if (seq.isEmpty) {
      acc.result()
    } else {
      val newSum = sumUntilNow + seq.head.integral(timeUnit)
      integrateMe(newSum, seq.tail, acc += seq.head.map(_ => newSum))(timeUnit)
    }
  }

  /**
    * @param entries entries over which to do a sliding sum
    * @param window width of the window over which we sum
    * @return a sequence of entries representing the sum of each entry within the window over time.
    *         NOTE: Entries that are only partially within the window are counted entirely
    */
  // TODO: return output guaranteed to be compressed
  // TODO: implement a "real" integral as this is darn imprecise.
  def slidingIntegral[T](
      entries: Seq[TSEntry[T]],
      window: Long,
      timeUnit: TimeUnit = TimeUnit.MILLISECONDS
  )(implicit n: Numeric[T]): Seq[TSEntry[Double]] =
    if (window < 1) {
      throw new IllegalArgumentException("Window must be strictly positive. Was " + window)
    } else if (entries.isEmpty) {
      Seq.empty
    } else {
      // Initialise the recursion
      slideMySum[T](
        entries,
        // TODO: passing 'entries' here could be made to work as well,
        // as long as we just return the sum result
        Seq(),
        .0, // Sum starts at 0
        entries.head.timestamp, // initial entry's timestamp
        new ArrayBuffer() // an empty accumulator
      )(
        window,
        entries.last.definedUntil,
        timeUnit
      )
    }

  @tailrec
  def slideMySum[T](
      remaining: Seq[TSEntry[T]],
      inWindow: Seq[TSEntry[T]],
      previousIntegral: Double,
      windowHeadTime: Long,
      acc: Builder[TSEntry[Double], Seq[TSEntry[Double]]]
  )(
      windowLength: Long,
      endOfTime: Long,
      timeUnit: TimeUnit
  )(
      implicit n: Numeric[T]
  ): Seq[TSEntry[Double]] = {
    // For now we stop here
    if (windowHeadTime == endOfTime) acc.result()
    else {
      val windowTailTime = windowHeadTime - windowLength + 1

      val (nextRemaining, nextWindow, updatedIntegral) =
        updateCollectionsAndSum(remaining, inWindow, previousIntegral, windowHeadTime, windowTailTime, timeUnit)

      // For how long is the new window content valid?
      val nextHeadTime =
        nextSumWindowAdvance(nextRemaining, nextWindow, windowHeadTime, windowTailTime)(endOfTime)

      if (nextWindow.nonEmpty) {
        // Currently only add an entry to the result if the current window is non-empty
        // TODO think about the merits of doing this?
        // This is both so that we have a 'unit operator when window is size 1' as well
        // as having no defined output when the window is completely over undefined input
        // At some point in the future we may want to let the user control what happens here.
        acc += TSEntry(windowHeadTime, updatedIntegral, nextHeadTime - windowHeadTime)
      }

      // Down the rabbit hole, baby
      slideMySum(
        nextRemaining,
        nextWindow,
        updatedIntegral,
        nextHeadTime,
        acc
      )(windowLength, endOfTime, timeUnit)
    }
  }

  /**
    * @return how far we may advance the window until we reach a point where
    *         we need to add or remove something, or we reach the end of the
    *         domain we wish to compute a sliding window for.
    */
  def nextSumWindowAdvance[T](
      remaining: Seq[TSEntry[T]],
      inWindow: Seq[TSEntry[T]],
      windowHeadTime: Long,
      windowTailTime: Long
  )(endOfTime: Long): Long = {

    val timeToNextEnteringOrEnd =
      // Time to next element entering the window, or time to end of domain of interest, if no elements are left to add
      remaining.headOption.map(_.timestamp).getOrElse(endOfTime) - windowHeadTime

    windowHeadTime + Math.min(
      timeToNextEnteringOrEnd,
      // Time to next element leaving the window, if any
      inWindow.headOption.map(_.definedUntil - windowTailTime).getOrElse(Long.MaxValue)
    )
  }

  /**
    * Return a (remaining, inWindow) adapted from the passed 'remaining'
    * and 'inWindow' entries according to the specified window head and tail times.
    */
  def updateCollectionsAndSum[T](
      remaining: Seq[TSEntry[T]],
      inWindow: Seq[TSEntry[T]],
      currentIntegral: Double,
      windowHeadTime: Long,
      windowTailTime: Long,
      timeUnit: TimeUnit
  )(implicit n: Numeric[T]): (Seq[TSEntry[T]], Seq[TSEntry[T]], Double) = {

    (remaining.headOption, inWindow.headOption) match {
      // Both window head and tail are defined, and reached a new entry and an existing entry's end
      case (Some(next), Some(last))
          if next.timestamp == windowHeadTime
          && last.definedUntil == windowTailTime =>
        (
          remaining.tail,        // Remove head from remaining
          inWindow.tail :+ next, // Keep current window's tail (head needs to be removed)
          // and add remaining's head
          currentIntegral - last.integral(timeUnit) + next.integral(timeUnit)
        )

      case (Some(next), _) if next.timestamp == windowHeadTime =>
        // the window head reached an entry which now needs to be added
        (
          remaining.tail, // Remove head from remaining
          inWindow :+ next, // Add remaining's head to current window content
          currentIntegral + next.integral(timeUnit)
        )
      case (_, Some(last)) if last.definedUntil == windowTailTime =>
        // the window tail reached the end of an entry's domain: it needs to be removed
        (
          remaining, // remaining remains as-is
          inWindow.tail, // window content is trimmed from its first element
          currentIntegral - inWindow.head.integral(timeUnit)
        )
      case _ =>
        throw new IllegalArgumentException("expecting exact boundary matches")
    }
  }
}
