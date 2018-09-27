package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.TSEntry

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, Builder}

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
  def stepIntegral[T](seq: Seq[TSEntry[T]])(implicit n: Numeric[T]): Seq[TSEntry[BigDecimal]] =
    if (seq.isEmpty) {
      Seq()
    } else {
      val zero = 0
      integrateMe[T](
        BigDecimal(zero),
        seq,
        new ArrayBuffer[TSEntry[BigDecimal]](seq.size)
      )(n)
    }

  @tailrec
  private def integrateMe[T](
                              sumUntilNow: BigDecimal,
                              seq: Seq[TSEntry[T]],
                              acc: Builder[TSEntry[BigDecimal], Seq[TSEntry[BigDecimal]]]
                            )(implicit n: Numeric[T]): Seq[TSEntry[BigDecimal]] = {
    if (seq.isEmpty) {
      acc.result()
    } else {
      val newSum = sumUntilNow + mult(seq.head.value, seq.head.validity)
      integrateMe(newSum, seq.tail, acc += (seq.head.map(_ => newSum)))
    }
  }

  //TODO: this is ugly as s***, check what's possible with Numeric to avoid .toDouble conversion.
  def mult[T](a: T, b: Long)(implicit n: Numeric[T]): BigDecimal = {
    import n._
    BigDecimal(a.toDouble()) * BigDecimal(b)
  }

}