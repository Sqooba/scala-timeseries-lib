package io.sqooba.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.timeseries.immutable.TSEntry

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, Builder}
import scala.concurrent.duration.TimeUnit

object NumericTimeSeries {

  /**
    * Defensive 'plus' operator: wherever one of the time series
    * is  undefined, the result is undefined.
    */
  def strictPlus[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
  : Option[T] = {
    import n._
    (lhO, rhO) match {
      case (Some(l), Some(r)) => Some(l + r)
      case _ => None
    }
  }

  /**
    * Defensive 'minus' operator: wherever one of the time series
    * is  undefined, the result is undefined.
    */
  def strictMinus[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
  : Option[T] = {
    import n._
    (lhO, rhO) match {
      case (Some(l), Some(r)) => Some(l - r)
      case _ => None
    }
  }

  /**
    * Defensive multiplication operator: wherever one of the time series
    * is  undefined, the result is undefined.
    */
  def strictMultiply[T](lhO: Option[T], rhO: Option[T])(implicit n: Numeric[T])
  : Option[T] = {
    import n._
    (lhO, rhO) match {
      case (Some(l), Some(r)) => Some(l * r)
      case _ => None
    }
  }

  def rolling[T](ts: TimeSeries[T], aggregator: Seq[T] => T, windowMs: Long)(implicit n: Numeric[T]): TimeSeries[T] =
    ts.mapWithTime { (time, currentVal) =>
      // values from the last `windowMs` milliseconds plus the current val
      aggregator(
        ts.slice(time - windowMs, time).entries.map(_.value) :+ currentVal
      )
    }

  /**
    * Compute an integral of the passed entries, such that each entry is equal
    * to its own value plus the sum of the entries that came before.
    *
    *
    * Please note that the result is still a step function.
    */
  def stepIntegral[T](seq: Seq[TSEntry[T]], timeUnit: TimeUnit = TimeUnit.MILLISECONDS)
                     (implicit n: Numeric[T]): Seq[TSEntry[Double]] =
    if (seq.isEmpty) {
      Seq()
    } else {
      integrateMe[T](
        .0,
        seq,
        new ArrayBuffer[TSEntry[Double]](seq.size))(timeUnit)(n)
    }

  @tailrec
  private def integrateMe[T](
                              sumUntilNow: Double,
                              seq: Seq[TSEntry[T]],
                              acc: Builder[TSEntry[Double], Seq[TSEntry[Double]]]
                            )
                            (timeUnit: TimeUnit)
                            (implicit n: Numeric[T]): Seq[TSEntry[Double]] = {
    if (seq.isEmpty) {
      acc.result()
    } else {
      val newSum = sumUntilNow + seq.head.integral(timeUnit)
      integrateMe(newSum, seq.tail, acc += (seq.head.map(_ => newSum)))(timeUnit)
    }
  }

}