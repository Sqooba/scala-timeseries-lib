package io.sqooba.oss.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.window.{Aggregator, WindowSlider}

import scala.annotation.tailrec
import scala.collection.mutable
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

  // TODO migrate this to the windowed stuff (and check if it's used anyhow)
  @deprecated("Please use the new windowing functions. See WindowSlider.scala")
  def rolling[T](
      ts: TimeSeries[T],
      aggregator: Seq[T] => T,
      windowMs: Long,
      compress: Boolean = true
  )(implicit n: Numeric[T]): TimeSeries[T] =
    ts.mapEntries(
      {
        case TSEntry(time, currentVal, _) =>
          // values from the last `windowMs` milliseconds plus the current val
          aggregator(
            ts.slice(time - windowMs, time).entries.map(_.value) :+ currentVal
          )
      },
      compress
    )

  /** Compute an integral of the passed entries, such that each entry is equal to its
    * own value plus the sum of the entries that came before. The time is integrated
    * as seconds, you may provide the unit with which the validities are converted to
    * seconds.
    *
    * @note The result is still a step function.
    *
    * @param entries  entries over which to integrate
    * @param timeUnit time unit used to convert validities to seconds
    */
  def stepIntegral[T](entries: Seq[TSEntry[T]], timeUnit: TimeUnit = TimeUnit.MILLISECONDS)(implicit n: Numeric[T]): Seq[TSEntry[Double]] =
    integrateMe[T](.0, entries, Seq.newBuilder)(timeUnit)(n)

  @tailrec
  private def integrateMe[T](
      sumUntilNow: Double,
      seq: Seq[TSEntry[T]],
      acc: mutable.Builder[TSEntry[Double], Seq[TSEntry[Double]]]
  )(timeUnit: TimeUnit)(implicit n: Numeric[T]): Seq[TSEntry[Double]] = {
    if (seq.isEmpty) {
      acc.result()
    } else {
      val newSum = sumUntilNow + seq.head.integral(timeUnit)
      integrateMe(newSum, seq.tail, acc += seq.head.map(_ => newSum))(timeUnit)
    }
  }

  /** First samples the entries and then calculates a sliding integral. This means
    * the resulting entries represent the value of the integral over the window `[t -
    * window, t]` of the original series.
    *
    * The sampling controls the resolution of the returned entries and hence the
    * (im-)precision of the integral. Therefore, the sampling rate cannot be larger
    * than the window size. Otherwise, the output becomes very un-intuitive and will
    * generally not make any sense.
    *
    * @note For optimal (im-)precision and maximally intuitive output, a `window` that
    * is a multiple of the `sampleRate` is recommended.
    *
    * @param entries  entries over which to integrate
    * @param window   width of the window
    * @param timeUnit time unit used to convert validities to seconds
    * @param sampleRate frequency of resampling
    * @return a sequence of entries representing the integral over the windows
    */
  def slidingIntegral[T](
      entries: Seq[TSEntry[T]],
      window: Long,
      sampleRate: Long,
      timeUnit: TimeUnit = TimeUnit.MILLISECONDS
  )(implicit n: Numeric[T]): Seq[TSEntry[Double]] = {
    require(window >= sampleRate, "The window must be as least as large as the sample rate.")

    WindowSlider
      .window(
        entries.toStream,
        window,
        Aggregator.integral[T](timeUnit),
        sampleRate
      )
      .map {
        // Drop the content of the window, just keep the integral's result.
        case (entry, integral) => entry.map[Double](_ => integral.get)
      }
  }

}
