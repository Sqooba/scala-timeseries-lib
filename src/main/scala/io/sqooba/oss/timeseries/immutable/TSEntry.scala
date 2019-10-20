package io.sqooba.oss.timeseries.immutable

import java.util.concurrent.TimeUnit

import io.sqooba.oss.timeseries.{TimeSeries, TimeSeriesMerger}

import scala.reflect.runtime.universe._

/**
  * Represents a time-series entry on the time-line, including its validity.
  *
  * Can also be used as a 'constant' time-series that has a single value.
  *
  * 'specialized' is used to have non-generic implementations for primitive types,
  *  which tend to be used a lot, in order to reduce the memory pressure a little bit.
  */
case class TSEntry[@specialized +T](timestamp: Long, value: T, validity: Long) extends TimeSeries[T] {

  require(validity > 0, s"Validity must be strictly positive ($validity was given)")

  def at(t: Long): Option[T] =
    if (defined(t)) {
      Some(value)
    } else {
      None
    }

  /** Return this entry within an option if it is valid at time t */
  def entryAt(t: Long): Option[TSEntry[T]] =
    if (defined(t)) {
      Some(this)
    } else {
      None
    }

  def size: Int = 1

  def isEmpty: Boolean = false

  def isCompressed: Boolean = true

  /** Shorten this entry's validity if it exceed 'at'. No effect otherwise.
    *
    * If the entry's timestamp is after 'at', the entry remains unchanged.
    */
  def trimRight(at: Long): TimeSeries[T] =
    if (at <= timestamp) { // Trim before or exactly on value start: result is empty.
      EmptyTimeSeries
    } else { // Entry needs to have its validity adapted.
      trimEntryRight(at)
    }

  /**
    * Returns an empty entry if:
    *  - 'at' is before or at the exact begin boundary of this entry's domain
    *  - 'at' is within the entry and it must not be split in two
    */
  def trimRightDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    if (at <= timestamp || (defined(at) && !includeEntry)) {
      EmptyTimeSeries
    } else {
      this
    }

  /** Similar to trimLeft, but returns a TSEntry instead of a time series and throws
    * if 'at' is before the entry's timestamp. */
  def trimEntryRight(at: Long): TSEntry[T] =
    if (at <= timestamp) { // Trim before or exactly on value start: result is empty.
      throw new IllegalArgumentException(s"Attempting to trim right at $at before entry's domain has started ($timestamp)")
    } else if (at >= definedUntil) { // No effect, return this instance
      this
    } else {
      // Entry needs to have its validity adapted.
      TSEntry(timestamp, value, at - timestamp)
    }

  /** Move this entry's timestamp to 'at' and shorten the validity accordingly,
    * if this entry is defined at 'at'. */
  def trimLeft(at: Long): TimeSeries[T] =
    if (at >= definedUntil) { // Nothing left from the value on the right side of the trim
      EmptyTimeSeries
    } else {
      trimEntryLeft(at)
    }

  /**
    * Returns an empty entry if:
    *  - 'at' is after this entry's domain
    *  - 'at' is within the entry (but not equal to the timestamp) and it must not be split in two
    */
  def trimLeftDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    if (at >= definedUntil // After the domain: empty in any case
        || (at != timestamp && defined(at) && !includeEntry)) { // within the domain but not on the begin boundary
      EmptyTimeSeries
    } else {
      this
    }

  /** Similar to trimLeft, but returns a TSEntry instead of a time series and throws
    * if 'at' exceeds the entry's definition. */
  def trimEntryLeft(at: Long): TSEntry[T] =
    if (at >= definedUntil) { // Nothing left from the value on the right side of the trim
      throw new IllegalArgumentException(s"Attempting to trim left at $at after entry's domain has ended ($definedUntil)")
    } else if (at <= timestamp) {
      // Trim before or exactly on value start, right side remains unchanged
      this
    } else { // Entry needs to have its timestamp and validity adapted.
      TSEntry(at, value, definedUntil - at)
    }

  /** Equivalent to calling trimEntryLeft(l).trimEntryRight(r)
    * without the intermediary step. */
  def trimEntryLeftNRight(l: Long, r: Long): TSEntry[T] =
    if (l >= definedUntil) {
      throw new IllegalArgumentException(s"Attempting to trim left at $l after entry's domain has ended ($definedUntil)")
    } else if (r <= timestamp) {
      throw new IllegalArgumentException(s"Attempting to trim right at $r before entry's domain has started ($timestamp)")
    } else if (l >= r) {
      throw new IllegalArgumentException(s"Left time must be strictly lower than right time. Was: $l and $r")
    } else if (l <= timestamp && r >= definedUntil) {
      this
    } else {
      val start = Math.max(timestamp, l)
      TSEntry(start, value, Math.min(definedUntil, r) - start)
    }

  override def defined(at: Long): Boolean = at >= timestamp && at < definedUntil

  /** the last moment where this entry is valid, non-inclusive */
  def definedUntil: Long = timestamp + validity

  /** return true if this and the other entry have an overlapping domain of definition.
    * False if the domains are only contiguous. */
  def overlaps[O](other: TSEntry[O]): Boolean =
    this.timestamp < other.definedUntil && this.definedUntil > other.timestamp

  def toLeftEntry[O]: TSEntry[Either[T, O]] =
    TSEntry(timestamp, Left[T, O](value), validity)

  def toRightEntry[O]: TSEntry[Either[O, T]] =
    TSEntry(timestamp, Right[O, T](value), validity)

  /** Map value contained in this timeseries using the passed function */
  def map[O: WeakTypeTag](f: T => O, compress: Boolean = true): TSEntry[O] =
    TSEntry(timestamp, f(value), validity)

  def mapWithTime[O: WeakTypeTag](f: (Long, T) => O, compress: Boolean = true): TSEntry[O] =
    TSEntry(timestamp, f(timestamp, value), validity)

  def filter(predicate: TSEntry[T] => Boolean): TimeSeries[T] =
    if (predicate(this)) this else EmptyTimeSeries

  def filterValues(predicate: T => Boolean): TimeSeries[T] =
    if (predicate(this.value)) this else EmptyTimeSeries

  def fill[U >: T](whenUndef: U): TimeSeries[U] = this

  def entries: Seq[TSEntry[T]] = Seq(this)

  override def values: Seq[T] = Seq(value)

  /** Append the other entry to this one.
    * Any part of this entry that is defined for t > other.timestamp will be overwritten,
    * either by 'other' or by nothing if 'others's validity does not reach t.
    *
    * Notes:
    * - if 'other' has a timestamp before this value, only 'other' is returned.
    * - if compression is enabled 'other' will be compressed into 'this' if their domains overlap and their
    * values are strictly equal. In that case, this entry may be shrunk if 'other's
    * domain of definition ends before 'this' one.
    */
  def appendEntry[U >: T](other: TSEntry[U], compress: Boolean = true): Seq[TSEntry[U]] =
    if (other.timestamp <= timestamp) {
      Seq(other)
    } else {
      extendOrTrim(other, compress)
    }

  /**
    * Note: expects that 'other' has a timestamp after 'this'
    *
    * @return a Seq of this entry, extended until the end of validity of 'other',
    *         if their values are strictly equal and their domain overlap.
    *         A seq of 'this', trimmed to 'other's timestamp, and the other entry is returned otherwise.
    */
  private def extendOrTrim[U >: T](other: TSEntry[U], compress: Boolean): Seq[TSEntry[U]] =
    if (compress && other.timestamp <= this.definedUntil && this.value == other.value) {
      // If the values are the same, we need to check if the new entry is shortening
      // the previous one.
      val extension = other.definedUntil - this.definedUntil
      if (extension < 0) {
        Seq(this.trimEntryRight(other.definedUntil))
      } else {
        Seq(extendValidity(other.definedUntil - this.definedUntil))
      }
    } else {
      Seq(this.trimEntryRight(other.timestamp), other)
    }

  /** Prepend the other entry to this one.
    * Any part of this entry that is defined at t < other.definedUntil will be overwritten by the
    * other entry, or not be defined if t < other.timestamp */
  def prependEntry[U >: T](other: TSEntry[U]): Seq[TSEntry[U]] =
    if (other.timestamp >= definedUntil) { // Complete overwrite, return the other.
      Seq(other)
    } else if (other.definedUntil < definedUntil) {
      // Something from this entry to be kept after the prepended one
      Seq(other, this.trimEntryLeft(other.definedUntil))
    } else { // the prepended entry completely overwrites the present one.
      Seq(other)
    }

  def head: TSEntry[T] = this

  def headOption: Option[TSEntry[T]] = Some(this)

  def last: TSEntry[T] = this

  def lastOption: Option[TSEntry[T]] = Some(this)

  /**
    * Creates a new entry with an extended validity.
    *
    * @param validityIncrement The validity increment
    */
  def extendValidity(validityIncrement: Long): TSEntry[T] =
    if (validityIncrement < 0) {
      throw new IllegalArgumentException(s"Cannot reduce validity of an entry ($this) with increment $validityIncrement.")
    } else if (validityIncrement == 0) {
      this
    } else {
      TSEntry(
        timestamp,
        value,
        validity + validityIncrement
      )
    }

  override def splitEntriesLongerThan(entryMaxLength: Long): TimeSeries[T] = {
    require(entryMaxLength > 0, "The max length of entries must be > 0")

    // We specifically do not want to compress when we split up entries
    val builder = newBuilder[T](compress = false)
    def streamTimeStamps(start: Long) =
      Stream
        .from(0)
        .map(i => start + i * entryMaxLength)

    // streams new timestamp until we reached the end of the ts
    streamTimeStamps(this.timestamp)
      .takeWhile(timestamp => timestamp < definedUntil)
      .foreach { timestamp =>
        // for each timestamp, build a ts entry, with a validity that is
        // at most entryMaxLength
        builder += TSEntry(
          timestamp,
          value,
          Math.min(
            entryMaxLength,
            definedUntil - timestamp
          )
        )
      }
    builder.result()
  }

  /**
    * Compute the integral of this entry. The caller may specify what time unit is used for this entry.
    *
    * By default, milliseconds are assumed.
    *
    * This function essentially computes "value * validity", with the validity first  being converted
    * to seconds according to the passed time unit.
    *
    * This currently returns a double value.
    *
    * TODO: return a (initVal, slope) tuple or something like an "IntegralEntry" instead ?
    */
  def integral[U >: T](timeUnit: TimeUnit = TimeUnit.MILLISECONDS)(implicit n: Numeric[U]): Double = {
    import n._

    // - Obtain the duration in milliseconds
    // - Compute the number of seconds (double division by 1000)
    // - Multiply by the value
    (TimeUnit.MILLISECONDS.convert(validity, timeUnit) / 1000.0) * value.toDouble
  }

  /**
    * See #integral
    */
  def integralEntry[U >: T](timeUnit: TimeUnit = TimeUnit.MILLISECONDS)(implicit n: Numeric[U]): TSEntry[Double] =
    this.map((_: T) => integral[U](timeUnit)(n))

  /**
    * The loose domain of an entry is simply its domain.
    *
    * @return The looseDomain of the time-series
    */
  def looseDomain: TimeDomain = ContiguousTimeDomain(timestamp, timestamp + validity)

  def supportRatio: Double = 1

  def isDomainContinuous: Boolean = true
}

object TSEntry {

  def apply[T](tup: (Long, T, Long)): TSEntry[T] =
    TSEntry(tup._1, tup._2, tup._3)

  /**
    * Define an implicit ordering for TSEntries of any type.
    * TSEntryOrdering extends the correct type but as Ordering[T] is invariant
    * we still need to enforce this type here.
    */
  implicit def orderByTs[T]: Ordering[TSEntry[T]] = TSEntryOrdering.asInstanceOf[Ordering[TSEntry[T]]]

  /** Merge two entries.
    * The domain covered by the returned entries (including a potential discontinuities)
    * will be between min(a.timestamp, b.timestamp) and max(a.definedUntil, b.definedUntil) */
  def merge[A, B, R](a: TSEntry[A], b: TSEntry[B])(op: (Option[A], Option[B]) => Option[R]): Seq[TSEntry[R]] =
    if (!a.overlaps(b)) {
      mergeDisjointDomain(a, b)(op)
    } else {
      mergeOverlapping(a, b)(op)
    }

  /** Merge two overlapping TSEntries and return the result as an
    * ordered sequence of TSEntries.
    *
    * This method returns a Seq containing one to three TSEntries defining a timeseries valid from
    *  first.timestamp to max(first.validUntil, second.validUntil).
    *    - one entry if first and second share the exact same domain
    *    - two entries if first and second share one bound of their domain,
    *    - three entries if the domains overlap without sharing a bound
    *
    * If the passed merge operator is commutative, then the 'merge' function is commutative as well.
    * (merge(op)(E_a,E_b) == merge(op)(E_b,E_a) only if op(a,b) == op(b,a))
    */
  protected def mergeOverlapping[A, B, R](a: TSEntry[A], b: TSEntry[B])(op: (Option[A], Option[B]) => Option[R]): Seq[TSEntry[R]] = {
    // Handle first 'partial' definition
    (Math.min(a.timestamp, b.timestamp), Math.max(a.timestamp, b.timestamp)) match {
      case (from, to) if from != to =>
        // Compute the result of the merge operation for a partially
        // defined input (either A or B is undefined for this segment)
        mergeValues(a, b)(from, to)(op)
      case _ => Seq.empty // a and b start at the same time. Nothing to do
    }
  } ++ {
    // Merge the two values over the overlapping domain of definition of a and b.
    (Math.max(a.timestamp, b.timestamp), Math.min(a.definedUntil, b.definedUntil)) match {
      case (from, to) if from < to => mergeValues(a, b)(from, to)(op)
      case _ =>
        throw new IllegalArgumentException(s"This function cannot merge non-overlapping entries: $a and $b")
    }
  } ++ {
    // Handle trailing 'partial' definition
    (Math.min(a.definedUntil, b.definedUntil), Math.max(a.definedUntil, b.definedUntil)) match {
      case (from, to) if from != to => mergeValues(a, b)(from, to)(op)
      case _                        => Seq.empty; // Entries end at the same time, nothing to do.
    }
  }

  /** Merge two entries that have a disjoint domain.
    * The merge operator will be applied to each individually
    */
  protected def mergeDisjointDomain[A, B, R](a: TSEntry[A], b: TSEntry[B])(op: (Option[A], Option[B]) => Option[R]): Seq[TSEntry[R]] =
    if (a.overlaps(b)) {
      throw new IllegalArgumentException(s"Function cannot be applied to overlapping entries: $a and $b")
    } else {
      op(Some(a.value), None).map(TSEntry(a.timestamp, _, a.validity)).toSeq ++
        TimeSeriesMerger.applyEmptyMerge(Math.min(a.definedUntil, b.definedUntil), Math.max(a.timestamp, b.timestamp))(op).toSeq ++
        op(None, Some(b.value)).map(TSEntry(b.timestamp, _, b.validity)).toSeq
    }.sorted

  /** Convenience function to merge the values present in the entries at time 'at' and
    * create an entry valid until 'until' from the result, if the merge operation is defined
    * for the input. */
  private def mergeValues[A, B, R](a: TSEntry[A], b: TSEntry[B])(at: Long, until: Long)(op: (Option[A], Option[B]) => Option[R]): Seq[TSEntry[R]] =
    op(a.at(at), b.at(at)).map(TSEntry(at, _, until - at)).toSeq
}
