package io.sqooba.oss.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.windowing.{IntegratingAggregator, WindowSlider}

import scala.annotation.tailrec
import scala.collection.mutable.Builder
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
      integrateMe[T](.0, seq, Seq.newBuilder)(timeUnit)(n)
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
    WindowSlider
      .window(
        entries.toStream,
        window,
        new IntegratingAggregator[T](timeUnit)
      )
      .map {
        // Drop the content of the window, just keep the integral's result.
        case (entry, integral) => entry.map[Double](_ => integral.get)
      }

}
