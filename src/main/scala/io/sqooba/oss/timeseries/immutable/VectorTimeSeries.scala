package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.validation.TSEntryFitter
import io.sqooba.oss.timeseries.{TimeSeries, TimeSeriesBuilder}

import scala.annotation.tailrec
import scala.collection.immutable.VectorBuilder
import scala.reflect.runtime.universe._

/**
  * TimeSeries implementation based on a Vector.
  *
  * @note data needs to be SORTED
  */
case class VectorTimeSeries[+T] private (
    data: Vector[TSEntry[T]],
    isCompressed: Boolean = false,
    isDomainContinuous: Boolean = false
) extends TimeSeries[T] {

  require(
    data.size >= 2,
    "A VectorTimeSeries can not be empty (should be an EmptyTimeSeries) nor contain only one element (should be a TSEntry)"
  )

  /**
    * Dichotomic search for the element in the time series for the entry
    * with the biggest timestamp lower or equal to 't'.
    * If an entry exists and it is valid at 't', Some(value) is returned.
    */
  def at(t: Long): Option[T] =
    entryAt(t).map(_.value)

  def entryAt(t: Long): Option[TSEntry[T]] = lastEntryAt(t).flatMap(_._1.entryAt(t))

  /**
    * Return the entry in the timeseries with the highest timestamp lower or equal to 't',
    * along with its index in the vector.
    */
  def lastEntryAt(t: Long): Option[(TSEntry[T], Int)] =
    VectorTimeSeries.dichotomicSearch(data, t)

  def mapWithTime[O: WeakTypeTag](f: (Long, T) => O, compress: Boolean = true): TimeSeries[O] =
    data
      .foldLeft(newBuilder[O](compress))((b, n) => b += n.mapWithTime(f))
      .result()

  def filter(predicate: TSEntry[T] => Boolean): TimeSeries[T] =
    // We are not updating entries: no need to order or trim them
    TimeSeries.ofOrderedEntriesUnsafe(this.data.filter(predicate))

  lazy val size: Int = data.size

  def isEmpty: Boolean = false

  def trimRight(t: Long): TimeSeries[T] =
    lastEntryAt(t - 1) match {
      case Some((e, 0)) =>
        // First element: trim and return it
        e.trimRight(t)
      case Some((e, idx)) =>
        data.splitAt(idx) match {
          // First of the last elements is valid and may need trimming. Others can be forgotten.
          case (noChange, _) =>
            new VectorTimeSeries(noChange :+ e.trimEntryRight(t))
        }
      case _ => EmptyTimeSeries
    }

  def trimRightDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    lastEntryAt(at - 1) match {
      case Some((e, 0)) =>
        // First element: trim and return it
        e.trimRightDiscrete(at, includeEntry)
      case Some((e, idx)) =>
        data.splitAt(idx) match {
          case (noChange, _) =>
            // First of the last elements must either be kept entirely or discarded
            TimeSeries.ofOrderedEntriesUnsafe(noChange ++ e.trimRightDiscrete(at, includeEntry).entries)
        }
      case _ => EmptyTimeSeries
    }

  def trimLeft(t: Long): TimeSeries[T] =
    if (data.head.timestamp >= t) {
      this
    } else {
      lastEntryAt(t) match {
        case Some((e, idx)) =>
          data.splitAt(idx) match {
            case (_, _ +: keep) =>
              if (e.defined(t)) {
                TimeSeries.ofOrderedEntriesUnsafe(e.trimEntryLeft(t) +: keep)
              } else if (keep.nonEmpty) {
                TimeSeries.ofOrderedEntriesUnsafe(keep)
              } else {
                EmptyTimeSeries
              }
          }
        case _ => EmptyTimeSeries
      }
    }

  def trimLeftDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    if (data.head.timestamp >= at) {
      this
    } else {
      lastEntryAt(at) match {
        case Some((e, idx)) =>
          data.splitAt(idx) match {
            case (_, _ +: keep) =>
              TimeSeries.ofOrderedEntriesUnsafe(
                e.trimLeftDiscrete(at, includeEntry).entries ++ keep
              )
          }
        case _ => EmptyTimeSeries
      }
    }

  def entries: Seq[TSEntry[T]] = data

  def headOption: Option[TSEntry[T]] = data.headOption

  def lastOption: Option[TSEntry[T]] = data.lastOption

  def looseDomain: TimeDomain =
    data.head.looseDomain.looseUnion(data.last.looseDomain)

  lazy val supportRatio: Double =
    data.map(_.looseDomain.size).sum.toFloat / looseDomain.size
}

object VectorTimeSeries {

  /**
    * Construct a VectorTimeSeries using its own builder given an ordered list of
    * at least 2 entries.
    *
    * @param elems A well-formed sequence of TSEntries which has at least 2 elements
    * @param compress specifies whether the entries should be compressed or not
    * @return a vector backed timeseries
    */
  private[timeseries] def ofOrderedEntriesSafe[T](
      elems: Seq[TSEntry[T]],
      compress: Boolean = true
  ): VectorTimeSeries[T] = {
    require(elems.size >= 2, "To build a VectorTimeSeries, at least two elements must be given.")

    // we can cast because we know the builder will output a vector series for more than 2 elements
    elems.foldLeft(new Builder[T]())(_ += _).result().asInstanceOf[VectorTimeSeries[T]]
  }

  /**
    * @param elems The entries of the series.
    * @param isCompressed A flag saying whether the elems have been compressed during construction.
    * @param isDomainContinuous Flags whether the elems span a continuous time domain without holes.
    * @return a VectorTimeSeries built from the passed entries, applying strictly no sanity check:
    *         use at your own risk.
    */
  private[timeseries] def ofOrderedEntriesUnsafe[T](
      elems: Seq[TSEntry[T]],
      isCompressed: Boolean = false,
      isDomainContinuous: Boolean = false
  ): VectorTimeSeries[T] =
    new VectorTimeSeries(elems.toVector, isCompressed, isDomainContinuous)

  /**
    * Run a dichotomic search on the passed sequence to find the entry in the
    * sequence that has the highest timestamp that is lower or equal to 'ts'.
    *
    * Some(entry, index) is returned if such an entry exists, None otherwise.
    */
  def dichotomicSearch[T](data: IndexedSeq[TSEntry[T]], ts: Long): Option[(TSEntry[T], Int)] =
    // Dichotomic search for a candidate
    dichotomic(data, ts, 0, data.size - 1) match {
      // Result is either 0, or the search failed
      case 0 => data.headOption.filter(_.timestamp <= ts).map((_, 0))
      case i: Int =>
        data(i) match {
          case e: TSEntry[T] if e.timestamp <= ts => Some((e, i))
          case _                                  => Some(data(i - 1), i - 1)
        }
    }

  /**
    * Dichotomic search within the passed Seq.
    *
    * Returns the index for an entry having a timestamp less or equal to the target.
    *
    *  - The returned value may be:
    *    - the correct index
    *    - the correct index + 1
    *    - 0 if the search fails, or if the result is 0.
    *
    */
  @tailrec
  private def dichotomic[T](
      data: IndexedSeq[TSEntry[T]],
      target: Long,
      lower: Int,
      upper: Int,
      previousPivot: Int = 0 // Default to 0 for initial call
  ): Int = {
    if (lower > upper) {
      previousPivot
    } else {
      val newPivot = (lower + upper) / 2
      data(newPivot).timestamp match {
        case after: Long if after > target => // Pivot is after target: 'upper' becomes pivot - 1
          dichotomic(data, target, lower, newPivot - 1, newPivot)
        case _: Long => // Pivot is before target: 'lower' becomes pivot + 1
          dichotomic(data, target, newPivot + 1, upper, newPivot)
      }
    }
  }

  private[timeseries] class Builder[T](compress: Boolean = true) extends TimeSeriesBuilder[T] {
    // Contains finalized entries
    private val resultBuilder = new VectorBuilder[TSEntry[T]]
    private val entryBuilder  = new TSEntryFitter[T](compress)

    private var resultCalled = false

    def addOne(elem: TSEntry[T]): this.type = {
      entryBuilder.addAndFitLast(elem).foreach(resultBuilder += _)
      this
    }

    override def clear(): Unit = {
      resultBuilder.clear()
      entryBuilder.clear()
      resultCalled = false
    }

    override def result(): TimeSeries[T] = {
      if (resultCalled) {
        throw new IllegalStateException("Cannot call result more than once, unless the builder was cleared.")
      }

      entryBuilder.lastEntry.foreach(resultBuilder += _)
      resultCalled = true

      TimeSeries.ofOrderedEntriesUnsafe(
        resultBuilder.result(),
        isCompressed = compress,
        entryBuilder.isDomainContinuous
      )
    }

    def definedUntil: Option[Long] = entryBuilder.lastEntry.map(_.definedUntil)
  }
}
