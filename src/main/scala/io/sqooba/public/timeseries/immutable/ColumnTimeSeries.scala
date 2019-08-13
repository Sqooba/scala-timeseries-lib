package io.sqooba.public.timeseries.immutable

import java.util.concurrent.TimeUnit

import io.sqooba.public.timeseries.{NumericTimeSeries, TSEntryFitter, TimeSeries, TimeSeriesBuilder}

import scala.annotation.tailrec
import scala.collection.immutable.VectorBuilder
import scala.concurrent.duration.TimeUnit

/**
  * TimeSeries implementation based on a column-store of three Vectors, one for each field of TSEntry.
  * This implementation should be more memory-efficient than the vanilla VectorTimeSeries.
  *
  * @note the entries of the three vectors must be ordered in the same way. The element at the same index in the three
  *       of them represents a TSEntry.
  */
case class ColumnTimeSeries[+T] private (
    timestamps: Vector[Long],
    values: Vector[T],
    validities: Vector[Long],
    isCompressed: Boolean = false,
    isDomainContinuous: Boolean = false
) extends TimeSeries[T] {

  require(
    timestamps.size >= 2,
    "A ColumnTimeSeries can not be empty (should be an EmptyTimeSeries) nor contain only one element (should be a TSEntry)"
  )

  require(
    timestamps.size == values.size && values.size == validities.size,
    "All three column vectors need to have the same number of elements."
  )

  // return the entries as a Stream to avoid eagerly mapping them to TSEntries
  def entries: Stream[TSEntry[T]] = (timestamps, values, validities).zipped.toStream.map(TSEntry[T])

  /**
    * Dichotomic search for the element in the time series for the entry
    * with the biggest timestamp lowerBound or equal to 't'.
    * If an entry exists and it is valid at 't', Some(value) is returned.
    */
  def at(t: Long): Option[T] = entryAt(t).map(_.value)

  def entryAt(t: Long): Option[TSEntry[T]] = lastEntryAt(t).flatMap(_._1.entryAt(t))

  /**
    * Return the entry in the timeseries with the highest timestamp lower or equal to 't',
    * along with its index in the vector.
    */
  def lastEntryAt(t: Long): Option[(TSEntry[T], Int)] =
    ColumnTimeSeries
      .dichotomicSearch(timestamps, t)
      .map(index => (entryAtIndex(index), index))

  private def entryAtIndex(index: Int): TSEntry[T] = TSEntry(timestamps(index), values(index), validities(index))

  /**
    * returns true if at(t) would return Some(value)
    */
  def defined(t: Long): Boolean = at(t).isDefined

  def head: TSEntry[T] = entryAtIndex(0)

  def headOption: Option[TSEntry[T]] = Some(head)

  def headValue: T = head.value

  def headValueOption: Option[T] = headOption.map(_.value)

  def last: TSEntry[T] = entryAtIndex(timestamps.length - 1)

  def lastOption: Option[TSEntry[T]] = Some(last)

  def lastValue: T = last.value

  def lastValueOption: Option[T] = lastOption.map(_.value)

  def map[O](f: T => O, compress: Boolean = true): TimeSeries[O] =
    mapWithTime((_, value) => f(value), compress)

  def mapWithTime[O](f: (Long, T) => O, compress: Boolean = true): TimeSeries[O] = {
    val mappedVs = (timestamps, values).zipped.map(f)

    if (compress) {
      (timestamps, mappedVs, validities).zipped
        .foldLeft(newBuilder[O]())((builder, triple) => builder += TSEntry(triple))
        .result()
    } else {
      ColumnTimeSeries.ofColumnVectorsUnsafe((timestamps, mappedVs, validities))
    }
  }

  def filter(predicate: TSEntry[T] => Boolean): TimeSeries[T] =
    filterTriples((ts, value, valid) => predicate(TSEntry(ts, value, valid)))

  def filterValues(predicate: T => Boolean): TimeSeries[T] =
    filterTriples((_, value, _) => predicate(value))

  private def filterTriples(predicate: (Long, T, Long) => Boolean): TimeSeries[T] =
    // We are not updating entries: no need to order or trim them
    ColumnTimeSeries.ofColumnVectorsUnsafe(
      (timestamps, values, validities).zipped.filter(predicate)
    )

  def fill[U >: T](whenUndef: U): TimeSeries[U] =
    (timestamps, values, validities).zipped
      .foldLeft(newBuilder[U]()) {
        case (builder, (ts, va, vd)) =>
          // if the last entry does not extend until the next entry, we add a filler
          if (builder.definedUntil.exists(_ < ts)) {
            val fillerTs = builder.definedUntil.get
            builder += TSEntry(fillerTs, whenUndef, ts - fillerTs)
          }
          builder += TSEntry(ts, va, vd)
      }
      .result()

  lazy val size: Int = timestamps.size

  def isEmpty: Boolean = false

  // TODO unify the trim implementations

  def trimRight(at: Long): TimeSeries[T] =
    lastEntryAt(at - 1) match {
      case Some((elem, 0)) =>
        // Trim on the first element: only trim and return this element
        elem.trimRight(at)

      case Some((elem, idx)) =>
        // Trim the element on the cut and append it to the elements on the left of it
        val trimmedRightMostElem = elem.trimEntryRight(at)
        val (leftTs, _)          = timestamps.splitAt(idx)
        val (leftValues, _)      = values.splitAt(idx)
        val (leftDs, _)          = validities.splitAt(idx)

        ColumnTimeSeries.ofColumnVectorsUnsafe(
          (
            leftTs :+ trimmedRightMostElem.timestamp,
            leftValues :+ trimmedRightMostElem.value,
            leftDs :+ trimmedRightMostElem.validity
          ),
          isCompressed,
          isDomainContinuous
        )
      case _ => EmptyTimeSeries
    }

  def trimRightDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    lastEntryAt(at - 1) match {
      case Some((e, 0)) =>
        // Trim on the first element: only trim and return this element
        e.trimRightDiscrete(at, includeEntry)

      case Some((elem, idx)) =>
        // Trim the element on the cut and append it to the elements on the left of it
        val trimmedRightMostSeries = elem.trimRightDiscrete(at, includeEntry)
        val (leftTs, _)            = timestamps.splitAt(idx)
        val (leftValues, _)        = values.splitAt(idx)
        val (leftDs, _)            = validities.splitAt(idx)

        ColumnTimeSeries.ofColumnVectorsUnsafe(
          (
            leftTs ++ trimmedRightMostSeries.entries.map(_.timestamp),
            leftValues ++ trimmedRightMostSeries.entries.map(_.value),
            leftDs ++ trimmedRightMostSeries.entries.map(_.validity)
          ),
          isCompressed,
          isDomainContinuous
        )
      case _ => EmptyTimeSeries
    }

  def trimLeft(t: Long): TimeSeries[T] =
    if (timestamps.head >= t) {
      this
    } else {
      lastEntryAt(t) match {
        case Some((elem, idx)) =>
          val (_, _ +: rightTs) = timestamps.splitAt(idx)
          val (_, _ +: rightVs) = values.splitAt(idx)
          val (_, _ +: rightDs) = validities.splitAt(idx)

          if (elem.defined(t)) {
            val trimmedLeftMostElem = elem.trimEntryLeft(t)
            ColumnTimeSeries.ofColumnVectorsUnsafe(
              (
                trimmedLeftMostElem.timestamp +: rightTs,
                trimmedLeftMostElem.value +: rightVs,
                trimmedLeftMostElem.validity +: rightDs
              ),
              isCompressed,
              isDomainContinuous
            )
          } else if (rightTs.nonEmpty) {
            ColumnTimeSeries.ofColumnVectorsUnsafe((rightTs, rightVs, rightDs))
          } else {
            EmptyTimeSeries
          }
        case _ => EmptyTimeSeries
      }
    }

  def trimLeftDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    if (timestamps.head >= at) {
      this
    } else {
      lastEntryAt(at) match {
        case Some((elem, idx)) =>
          val (_, _ +: rightTs) = timestamps.splitAt(idx)
          val (_, _ +: rightVs) = values.splitAt(idx)
          val (_, _ +: rightDs) = validities.splitAt(idx)

          if (elem.defined(at)) {
            val trimmedLeftMostSeries = elem.trimLeftDiscrete(at, includeEntry)
            ColumnTimeSeries.ofColumnVectorsUnsafe(
              (
                trimmedLeftMostSeries.entries.map(_.timestamp).toVector ++ rightTs,
                trimmedLeftMostSeries.entries.map(_.value).toVector ++ rightVs,
                trimmedLeftMostSeries.entries.map(_.validity).toVector ++ rightDs
              ),
              isCompressed,
              isDomainContinuous
            )
          } else if (rightTs.nonEmpty) {
            ColumnTimeSeries.ofColumnVectorsUnsafe((rightTs, rightVs, rightDs))
          } else {
            EmptyTimeSeries
          }
        case _ => EmptyTimeSeries
      }
    }

  override def slidingIntegral[U >: T](
      window: Long,
      timeUnit: TimeUnit = TimeUnit.MILLISECONDS
  )(implicit n: Numeric[U]): TimeSeries[Double] =
    if (this.size < 2) {
      this.map(n.toDouble)
    } else {
      // TODO: have slidingSum return compressed output so we can use the unsafe constructor and save an iteration.
      // TODO: don't use entries but directly operate on the column vectors.
      ColumnTimeSeries
        .ofOrderedEntriesSafe(NumericTimeSeries.slidingIntegral[U](this.entries, window, timeUnit))
    }

  def looseDomain: TimeDomain = ContiguousTimeDomain(timestamps.head, timestamps.last + validities.last)

  lazy val supportRatio: Double = validities.sum.toDouble / looseDomain.size

  override def newBuilder[U](compress: Boolean = true): TimeSeriesBuilder[U] =
    ColumnTimeSeries.newBuilder(compress)
}

object ColumnTimeSeries {

  private[timeseries] def ofOrderedEntriesSafe[T](
      entries: Seq[TSEntry[T]],
      compress: Boolean = true
  ): TimeSeries[T] =
    entries.foldLeft(newBuilder[T](compress))(_ += _).result()

  /**
    * @param columns the built, SORTED and valid column vectors representing the entries of the timeseries. The first
    *                vector contains the timestamps, the second the values and the third the validities.
    * @param isCompressed A flag saying whether the elems have been compressed during construction.
    * @return a ColumnTimeSeries built from the passed entries, applying strictly no sanity check:
    *         use at your own risk.
    */
  private[timeseries] def ofColumnVectorsUnsafe[T](
      columns: (Vector[Long], Vector[T], Vector[Long]),
      isCompressed: Boolean = false,
      isDomainContinuous: Boolean = false
  ): TimeSeries[T] = {
    val (timestamps, values, validities) = columns

    timestamps.size match {
      case 0 => EmptyTimeSeries
      case 1 => TSEntry(timestamps.head, values.head, validities.head)
      case _ => new ColumnTimeSeries(timestamps, values, validities, isCompressed, isDomainContinuous)
    }
  }

  /**
    * Run a dichotomic search on the passed sequence of timestamps to find the highest
    * timestamp that is lower or equal to 'ts'.
    *
    * @param timestamps an indexed sequence of timestamps
    * @param targetTimestamp the timestamp to search for
    * @return Some(index) where index is the position of the timetamp in the sequence
    *         if such a timestamp exists, None otherwise.
    */
  def dichotomicSearch(timestamps: IndexedSeq[Long], targetTimestamp: Long): Option[Int] =
    dichotomic(timestamps, targetTimestamp, 0, timestamps.size - 1) match {
      // the result is 0, or the search failed
      case 0 => timestamps.headOption.filter(_ <= targetTimestamp).map(_ => 0)
      // the result is nonzero
      case i: Int =>
        timestamps(i) match {
          case correct: Long if correct <= targetTimestamp => Some(i)
          case _                                           => Some(i - 1)
        }
    }

  /**
    * Dichotomic search within the passed Seq of timestamps.
    *
    * @return the index for an entry having a timestamp less or equal to the targetTimestamp.
    *         This may be:
    *           - the correct index
    *           - the correct index + 1
    *           - 0 if the search fails, or if the result is 0.
    */
  @tailrec
  private def dichotomic[T](
      timestamps: IndexedSeq[Long],
      targetTimestamp: Long,
      lowerBound: Int,
      upperBound: Int,
      previousPivot: Int = 0 // Default to 0 for initial call
  ): Int = {
    if (lowerBound > upperBound) {
      previousPivot
    } else {
      val newPivot = (lowerBound + upperBound) / 2

      timestamps(newPivot) match {
        // Pivot is after targetTimestamp: 'upperBound' becomes pivot - 1
        case after: Long if after > targetTimestamp =>
          dichotomic(timestamps, targetTimestamp, lowerBound, newPivot - 1, newPivot)

        // Pivot is before targetTimestamp: 'lowerBound' becomes pivot + 1
        case _: Long =>
          dichotomic(timestamps, targetTimestamp, newPivot + 1, upperBound, newPivot)
      }
    }
  }

  /**
    * @return the builder for column-base timeseries
    */
  def newBuilder[T](compress: Boolean = true): TimeSeriesBuilder[T] = new ColumnTimeSeries.Builder[T](compress)

  private class Builder[T](compress: Boolean = true) extends TimeSeriesBuilder[T] {

    // Contains finalized entries
    private val resultBuilder = (new VectorBuilder[Long], new VectorBuilder[T], new VectorBuilder[Long])
    private val entryBuilder  = new TSEntryFitter[T](compress)

    private var resultCalled = false

    override def +=(elem: TSEntry[T]): this.type = {
      entryBuilder.addAndFitLast(elem).foreach(addToBuilder)
      this
    }

    override def clear(): Unit = {
      resultBuilder._1.clear()
      resultBuilder._2.clear()
      resultBuilder._3.clear()
      entryBuilder.clear()
      resultCalled = false
    }

    override def result(): TimeSeries[T] = {
      if (resultCalled) {
        throw new IllegalStateException("result can only be called once, unless the builder was cleared.")
      }

      entryBuilder.lastEntry.foreach(addToBuilder)
      resultCalled = true

      ColumnTimeSeries.ofColumnVectorsUnsafe(
        (resultBuilder._1.result, resultBuilder._2.result, resultBuilder._3.result),
        compress,
        entryBuilder.isDomainContinuous
      )
    }

    private def addToBuilder(entry: TSEntry[T]): Unit = {
      resultBuilder._1 += entry.timestamp
      resultBuilder._2 += entry.value
      resultBuilder._3 += entry.validity
    }

    def definedUntil: Option[Long] = entryBuilder.lastEntry.map(_.definedUntil)
  }
}
