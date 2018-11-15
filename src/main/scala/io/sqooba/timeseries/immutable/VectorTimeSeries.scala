package io.sqooba.timeseries.immutable

import io.sqooba.timeseries.TimeSeries
import io.sqooba.timeseries.TimeSeriesBuilder

import scala.annotation.tailrec

/**
  * TimeSeries implementation based on a Vector.
  *
  * Useful for working on time series like data when no random access is required,
  * as any method requiring some sort of lookup will only run in linear time.
  */
case class VectorTimeSeries[T](data: Vector[TSEntry[T]])
// data needs to be SORTED -> TODO: private constructor?
    extends TimeSeries[T] {

  /**
    * Dichotomic search for the element in the time series for the entry
    * with the biggest timestamp lower or equal to 't'.
    * If an entry exists and it is valid at 't', Some(value) is returned.
    */
  def at(t: Long): Option[T] =
    entryValidAt(t).map(_.value)

  def entryValidAt(t: Long): Option[TSEntry[T]] =
    if (data.isEmpty) {
      None
    } else {
      lastEntryAt(t).flatMap(_._1.entryAt(t))
    }

  /**
    * Return the entry in the timeseries with the highest timestamp lower or equal to 't',
    * along with its index in the vector.
    */
  def lastEntryAt(t: Long): Option[(TSEntry[T], Int)] =
    VectorTimeSeries.dichotomicSearch(data, t)

  /**
    * returns true if at(t) would return Some(value)
    */
  def defined(t: Long): Boolean = at(t).isDefined

  def map[O](f: T => O, compress: Boolean = true): TimeSeries[O] =
    if (compress) {
      // Use a builder to handle compression
      new VectorTimeSeries(
        data
          .foldLeft(new TimeSeriesBuilder[O]())((b, n) => b += n.map(f))
          .result()
      )
    } else {
      new VectorTimeSeries[O](data.map(_.map(f)))
    }

  def mapWithTime[O](f: (Long, T) => O, compress: Boolean = true): TimeSeries[O] =
    if (compress) {
      // Use a builder to handle compression
      new VectorTimeSeries(
        data
          .foldLeft(new TimeSeriesBuilder[O]())((b, n) => b += n.mapWithTime(f))
          .result()
      )
    } else {
      new VectorTimeSeries[O](data.map(_.mapWithTime(f)))
    }

  def filter(predicate: TSEntry[T] => Boolean): TimeSeries[T] =
    // We are not updating entries: no need to order or trim them
    this.data.filter(predicate) match {
      case Vector()              => EmptyTimeSeries()
      case v: Vector[TSEntry[T]] => VectorTimeSeries.ofEntriesUnsafe(v)
    }

  def filterValues(predicate: T => Boolean): TimeSeries[T] =
    filter(tse => predicate(tse.value))

  def fill(whenUndef: T): TimeSeries[T] =
    new VectorTimeSeries[T](TimeSeries.fillGaps(data, whenUndef).toVector)

  def size(): Int = data.size

  def trimLeft(t: Long): TimeSeries[T] =
    // Check obvious shortcuts
    if (data.isEmpty) {
      EmptyTimeSeries()
    } else if (data.size == 1) {
      data.head.trimLeft(t)
    } else if (data.head.timestamp >= t) {
      this
    } else {
      lastEntryAt(t) match {
        case Some((e, idx)) =>
          data.splitAt(idx) match {
            case (_, _ +: keep) =>
              if (e.defined(t)) {
                new VectorTimeSeries(e.trimEntryLeft(t) +: keep)
              } else if (!keep.isEmpty) {
                new VectorTimeSeries(keep)
              } else {
                EmptyTimeSeries()
              }
          }
        case _ => EmptyTimeSeries()
      }
    }

  def trimRight(t: Long): TimeSeries[T] =
    if (data.isEmpty) {
      EmptyTimeSeries()
    } else if (data.size == 1) {
      data.last.trimRight(t)
    } else {
      lastEntryAt(t - 1) match {
        case Some((e, 0)) => // First element: trim and return it
          e.trimRight(t)
        case Some((e, idx)) =>
          data.splitAt(idx) match {
            // First of the last elements is valid and may need trimming. Others can be forgotten.
            case (noChange, _) =>
              new VectorTimeSeries(noChange :+ e.trimEntryRight(t))
          }
        case _ => EmptyTimeSeries()
      }
    }

  def entries: Seq[TSEntry[T]] = data

  def head: TSEntry[T] = data.head

  def headOption: Option[TSEntry[T]] = data.headOption

  def headValue: T = data.head.value

  def headValueOption: Option[T] = data.headOption.map(_.value)

  def last: TSEntry[T] = data.last

  def lastOption: Option[TSEntry[T]] = data.lastOption

  def lastValue: T = data.last.value

  def lastValueOption: Option[T] = data.lastOption.map(_.value)

  def append(other: TimeSeries[T]): TimeSeries[T] =
    other.headOption match {
      case None => // other is empty, nothing to do.
        this
      case Some(tse) if tse.timestamp > head.timestamp => // Something to keep from the current TS
        VectorTimeSeries
          .ofEntriesUnsafe(this.trimRight(tse.timestamp).entries ++ other.entries)
      case _ => // Nothing to keep, other overwrites this TS completely
        other
    }

  def prepend(other: TimeSeries[T]): TimeSeries[T] =
    other.lastOption match {
      case None => // other is empty, nothing to do.
        this
      case Some(tse) if tse.definedUntil < last.definedUntil =>
        // Something to keep from the current TS
        VectorTimeSeries.ofEntriesUnsafe(
          other.entries ++
          this.trimLeft(tse.definedUntil).entries
        )
      case _ => // Nothing to keep, other overwrites this TS completely
        other
    }

  def resample(sampleLengthMs: Long): TimeSeries[T] =
    new VectorTimeSeries(
      this.entries.flatMap(e => e.resample(sampleLengthMs).entries).toVector
    )

}

object VectorTimeSeries {

  /**
    * @return  a VectorTimeSeries built from the passed entries, ensuring that they are:
    *          - sorted
    *          - fitted to each other (no overlaps)
    */
  def ofEntriesSafe[T](elems: Seq[TSEntry[T]]): VectorTimeSeries[T] =
    // TODO: Expect entries to be sorted and just check?
    // TODO: the fitting function returns a vector in most cases: don't rebuild one in such case
    new VectorTimeSeries(Vector(TimeSeries.fitAndCompressTSEntries(elems.sortBy(_.timestamp)): _*))

  /**
    * @return a VectorTimeSeries built from the passed entries, only ensuring that they are sorted
    */
  // TODO clarify why we want to sort here.
  private[timeseries] def ofEntriesUnsafe[T](elems: Seq[TSEntry[T]]): VectorTimeSeries[T] =
    new VectorTimeSeries(Vector(elems.sortBy(_.timestamp): _*))

  def apply[T](elems: (Long, (T, Long))*): VectorTimeSeries[T] =
    ofEntriesUnsafe(elems.map(t => TSEntry(t._1, t._2._1, t._2._2)))

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
          case e: TSEntry[T] if (e.timestamp <= ts) => Some((e, i))
          case _                                    => Some(data(i - 1), i - 1)
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
        case after: Long if (after > target) => // Pivot is after target: 'upper' becomes pivot - 1
          dichotomic(data, target, lower, newPivot - 1, newPivot)
        case _: Long => // Pivot is before target: 'lower' becomes pivot + 1
          dichotomic(data, target, newPivot + 1, upper, newPivot)
      }
    }
  }

}
