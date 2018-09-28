package io.sqooba.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.timeseries.immutable.{TSEntry, VectorTimeSeries}

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, Builder}
import scala.concurrent.duration.TimeUnit

trait TimeSeries[T] {

  /** The value valid at time 't' if there is one. */
  def at(t: Long): Option[T]

  /** Split this time series into two.
    *
    * Returns a tuple of two contiguous time series,
    * such that the left time series is never defined for t >= 'at'
    * and the right time series is never defined for t < 'at'.
    *
    * Default implementation simply returns (this.trimRight(at), this.trimLeft(at))
    */
  def split(at: Long): (TimeSeries[T], TimeSeries[T]) = (this.trimRight(at), this.trimLeft(at))

  /** Extract a slice from this time series.
    *
    * The returned slice will only be defined between the specified bounds such that:
    *
    *  this.at(x) == returned.at(x) for all x in [from, to[.
    *
    * If x is outside of the bounds, returned.at(x) is None.
    */
  def slice(from: Long, to: Long): TimeSeries[T] = this.trimLeft(from).trimRight(to)

  /** Returns a time series that is never defined for t >= at and unchanged for t < at */
  def trimRight(at: Long): TimeSeries[T]

  /** Returns a time series that is never defined for t < at and unchanged for t >= at */
  def trimLeft(at: Long): TimeSeries[T]

  /** The number of elements in this time-series. */
  def size(): Int

  /** True if this time series is defined at 'at'. Ie, at('at') would return Some[T] */
  def defined(at: Long): Boolean

  /** Map the values within the time series.
    * Timestamps and validities of entries remain unchanged */
  def map[O](f: T => O): TimeSeries[O]

  /** Map the values within the time series.
    * Timestamps and validities of entries remain unchanged,
    * but the time is made available for cases where the new value would depend on it. */
  def mapWithTime[O](f: (Long, T) => O): TimeSeries[O]

  /** Fill the wholes in the definition domain of this time series with the passed value.
    * The resulting time series will have a single continuous definition domain,
    * provided the original time series was non-empty. */
  def fill(whenUndef: T): TimeSeries[T]

  /** Return a Seq of the TSEntries representing this time series. */
  def entries: Seq[TSEntry[T]]

  /** Return the first (chronological) entry in this time series.
    *
    * @throws NoSuchElementException if this time series is empty. */
  def head: TSEntry[T]

  /** Return a filled option containing the first (chronological) entry in this
    * time series.
    * None if this time series is empty. */
  def headOption: Option[TSEntry[T]]

  /** Return the last (chronological) entry in this time series.
    *
    * @throws NoSuchElementException if this time series is empty. */
  def last: TSEntry[T]

  /** Return a filled option containing the last (chronological) entry in this
    * time series.
    * None if this time series is empty. */
  def lastOption: Option[TSEntry[T]]

  /** Append the 'other' time series to this one at exactly the first of other's entries timestamp.
    *
    * if t_app = other.head.timestamp, this time series domain will be completely forgotten for all
    * t > t_app, and replaced with whatever is in the domain of 'other'.
    *
    * This is equivalent to right-trimming this time series at other.head.timestamp and prepending
    * it as-is to 'other'.
    *
    * If 'other' is empty, this time series is unchanged.
    */
  def append(other: TimeSeries[T]): TimeSeries[T]

  /** Prepend the 'other' time series to this one at exactly the last of other's entries definedUntil().
    *
    * if t_prep = other.last.definedUntil, this time series domain will be completely forgotten for all
    * t <= t_prep, and replaced with whatever is in the domain of 'other'.
    *
    * This is equivalent to left-trimming this time series at other.last.definedUntil and appending
    * it as-is with to 'other'.
    *
    * If 'other' is empty, this time series is unchanged.
    */
  def prepend(other: TimeSeries[T]): TimeSeries[T]

  /**
    * Merge another time series to this one, using the provided operator
    * to merge entries.
    *
    * The operator can define all four cases encountered during merging:
    *   - both entries defined
    *   - only one of the entries defined
    *   - no entry defined
    *
    * In any case, the returned time series will only be defined between the
    * bounds defined by min(this.head.timestamp, other.head.timestamp) and
    * max(this.last.definedUntil, other.last.definedUntil)
    */
  def merge[O, R]
  (op: (Option[T], Option[O]) => Option[R])
  (other: TimeSeries[O])
  : TimeSeries[R] =
    VectorTimeSeries.ofEntriesUnsafe(
      TimeSeries.mergeEntries(this.entries)(other.entries)(op))

  /**
    * Sum the entries within this and the provided time series such that
    * this.at(x) + other.at(x) = returned.at(x) where x may take any value where
    * both time series are defined.
    */
  def plus(other: TimeSeries[T])(implicit n: Numeric[T]) =
    VectorTimeSeries.ofEntriesUnsafe(
      TimeSeries.mergeEntries(this.entries)(other.entries)(NumericTimeSeries.strictPlus(_, _)(n)))

  def +(other: TimeSeries[T])(implicit n: Numeric[T]) = plus(other)

  /**
    * Subtract the entries within this and the provided time series such that
    * this.at(x) - other.at(x) = returned.at(x) where x may take any value where
    * both time series are defined.
    */
  def minus(other: TimeSeries[T])(implicit n: Numeric[T]) =
    VectorTimeSeries.ofEntriesUnsafe(
      TimeSeries.mergeEntries(this.entries)(other.entries)(NumericTimeSeries.strictMinus(_, _)(n)))

  def -(other: TimeSeries[T])(implicit n: Numeric[T]) = minus(other)

  /**
    * Multiply the entries within this and the provided time series such that
    * this.at(x) * other.at(x) = returned.at(x) where x may take any value where
    * both time series are defined.
    */
  def multiply(other: TimeSeries[T])(implicit n: Numeric[T]) =
    VectorTimeSeries.ofEntriesUnsafe(
      TimeSeries.mergeEntries(this.entries)(other.entries)(NumericTimeSeries.strictMultiply(_, _)(n)))

  def *(other: TimeSeries[T])(implicit n: Numeric[T]) = multiply(other)

  /**
    * Zips this time series with another one, returning a time series of tuples containing the values from
    * both this and the other time series across their common domain.
    */
  def strictZip[O](other: TimeSeries[O]): TimeSeries[(T, O)] =
    merge(
      strictZipOp[T, O]
    )(other)

  private def strictZipOp[L, R](left: Option[L], right: Option[R]): Option[(L, R)] =
    (left, right) match {
      case (Some(l), Some(r)) => Some((l, r))
      case _ => None
    }

  /**
    * Computes the integral of this time series.
    * This function returns a step function, so only represents an approximation.
    * Use it if you need to compute multiple integrals of the same time series.
    */
  def stepIntegral(stepLengthMs: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS)(implicit n: Numeric[T]) =
    VectorTimeSeries.ofEntriesUnsafe(
      NumericTimeSeries.stepIntegral(
        this.resample(stepLengthMs).entries,
        timeUnit
      )
    )

  /**
    * Compute the integral of this time series between the two specified points.
    * Simply sums values on the [from, to] interval.
    * If you need to call this multiple times, consider using #stepIntegral()
    * depending on your use case.
    */
  def integrateBetween(from: Long, to: Long)(implicit n: Numeric[T]): T =
    this.slice(from, to).entries.map(_.value).sum

  /**
    * Resamples the time series by ensuring that each entry has a validity
    * of maximum sampleLengthMs.
    */
  def resample(sampleLengthMs: Long): TimeSeries[T]

  /**
    * Compute a new time series that will contain, for any query time t, the sum
    * of entries present in this time series that are defined for a time
    * between t - window and t.
    *
    * Note: returns a step function, meaning that there is a slight level of imprecision
    * depending on your resolution.
    * The bigger the window is relative to individual entries' durations, the smaller the imprecision becomes.
    *
    * @param window width of the sliding integration window
    * @return a TimeSeries that for any queried time will return an approximate integral of
    *         this time series over the past window
    */
  def slidingIntegral(
                       window: Long,
                       timeUnit: TimeUnit = TimeUnit.MILLISECONDS
                     )(implicit n: Numeric[T]): TimeSeries[Double] =
    if (this.size() < 2) {
      this.map(n.toDouble)
    } else {
      // TODO: have slidingSum return compressed output so we can use the unsafe constructor
      // and save an iteration
      VectorTimeSeries.ofEntriesSafe(NumericTimeSeries.slidingIntegral(this.entries, window, timeUnit))
    }

}

object TimeSeries {

  /**
    * Assumes the input entries to be ordered. This function will do both:
    *  - compressing of any contiguous or overlapping entries that have values that are strictly equal.
    *  - correctly fitting overlapping entries together when they are not equal.
    *
    * @return a sequence of TSEntries that are guaranteed not to overlap with each other,
    *         and where contiguous values are guaranteed to be different.
    */
  def fitAndCompressTSEntries[T](in: Seq[TSEntry[T]]): Seq[TSEntry[T]] =
    if (in.size < 2) {
      in
    } else {
      compressMe(in, new ArrayBuffer[TSEntry[T]](in.size))
    }

  @tailrec
  private def compressMe[T](in: Seq[TSEntry[T]], acc: Builder[TSEntry[T], Seq[TSEntry[T]]]): Seq[TSEntry[T]] = {
    in match {
      case Seq(first, last) =>
        // Only two elements remaining, we reached the end of the seq.
        // Compress the two values if required, add to the accumulator and return the result
        (acc ++= first.appendEntry(last)).result()
      case Seq(first, second, tail@_*) =>
        first.appendEntry(second) match {
          case Seq(compressed) =>
            // Compression occurred.
            // We recurse without adding to the accumulator,
            // as the next value may be compressed into this one as well
            compressMe(compressed +: tail, acc)
          case Seq(first, second) =>
            // No compression occurred:
            // the first element can be added to the accumulator,
            // we then recurse with the second one as the head of the seq,
            // as the following values may be compressed into it
            acc += first
            compressMe(second +: tail, acc)
        }
    }
  }

  /** For any collection of TSEntries of size 2 and more, intersperses entries containing
    * fillValue between any two non-contiguous entries.
    *
    * Assumes the passed entries to be both properly fitted (no overlapping domains) and
    * compressed (no contiguous entries containing the same value).
    *
    * The result will be properly fitted and compressed as well.
    */
  def fillGaps[T](in: Seq[TSEntry[T]], fillValue: T): Seq[TSEntry[T]] =
    if (in.size < 2) {
      return in
    } else {
      fillMe(in, fillValue, new ArrayBuffer[TSEntry[T]](in.size))
    }

  @tailrec
  private def fillMe[T](in: Seq[TSEntry[T]], fillValue: T, acc: Builder[TSEntry[T], Seq[TSEntry[T]]]): Seq[TSEntry[T]] =
    in match {
      case Seq(first, last) =>
        // Only two elements remaining: the recursion can end
        (acc ++= fillAndCompress(first, last, fillValue)).result()
      case Seq(first, second, tail@_*) =>
        // Fill the gap, and check the result
        fillAndCompress(first, second, fillValue) match {
          // the above may return 1, 2 or 3 entries,
          // of which the last one must not yet
          // be added to the accumulator,
          // instead it is prepended to what is passed to the recursive call
          case Seq(compressed) =>
            // Nothing to add to acc:
            // compressed may still be extended by the next filler
            fillMe(compressed +: tail, fillValue, acc)
          case Seq(one, two) =>
            // The fill value either extended 'first' or advanced 'second:
            // we don't need to know and just add first to acc
            fillMe(two +: tail, fillValue, acc += one)
          case Seq(_, filler, _) =>
            // the fill value did not extend the first,
            // and did not advance the second
            // first and filler are added to the accumulator
            fillMe(second +: tail, fillValue, acc ++= Seq(first, filler))
        }
    }

  /** Returns a Sequence of entries such that there is no discontinuity between current.timestamp
    * and next.definedUntil, filling the gap between the entries and compression them if necessary. */
  def fillAndCompress[T](first: TSEntry[T], second: TSEntry[T], fillValue: T): Seq[TSEntry[T]] = {
    if (first.definedUntil() == second.timestamp) {
      // Entries contiguous.
      Seq(first, second)
    } else {
      // There is space to fill
      first.appendEntry(
        TSEntry(
          first.definedUntil(),
          fillValue,
          second.timestamp - first.definedUntil())
      ) match {
        case Seq(single) =>
          // 'first' was extended.
          // // Check if 'second' can be compressed into the result
          single.appendEntry(second)
        case Seq(notExtended, filler) =>
          // 'first' was not extended.
          // Check if 'second' can be advanced with the filling value
          notExtended +: filler.appendEntry(second)
      }
    }
  }

  /** Merge two time series together, using the provided merge operator.
    *
    * The passed TSEntry sequences will be merged according to the merge operator,
    * which will always be applied to one of the following:
    *    - two defined TSEntries with exactly the same domain of definition
    *    - a defined entry from A and None from B
    *    - a defined entry from B and None from A
    *    - No defined entry from A nor B.
    *
    * Overlapping TSEntries in the sequences a and b are trimmed to fit
    * one of the aforementioned cases before being passed to the merge function.
    *
    * For example,
    *    - if 'x' and '-' respectively represent the undefined and defined parts of a TSEntry
    *    - '|' delimits the moment on the time axis where a change in definition occurs either
    * in the present entry or in the one with which it is currently being merged
    *    - 'result' is the sequence resulting from the merge
    *
    * We apply the merge function in the following way:
    *
    * a_i:    xxx|---|---|xxx|xxx
    * b_j:    xxx|xxx|---|---|xxx
    *
    * result: (1) (2) (3) (4) (5)
    *
    * (1),(5) : op(None, None)
    * (2) : op(Some(a_i.value), None)
    * (3) : op(Some(a_i.value), Some(b_j.value))
    * (4) : op(None, Some(b_j.value))
    *
    * Assumes a and b to be ORDERED!
    */
  def mergeEntries[A, B, C]
  (a: Seq[TSEntry[A]])
  (b: Seq[TSEntry[B]])
  (op: (Option[A], Option[B]) => Option[C])
  : Seq[TSEntry[C]] =
  // TODO: consider moving the compression within the merging logic so we avoid a complete iteration.
    fitAndCompressTSEntries(
      mergeEithers(Seq.empty)(mergeOrderedSeqs(a.map(_.toLeftEntry[B]), b.map(_.toRightEntry[A])))(op)
    )

  /**
    * Combine two Seq's that are known to be ordered and return a Seq that is
    * both ordered and that contains both of the elements in 'a' and 'b'.
    * Adapted from http://stackoverflow.com/a/19452304/1997056
    */
  def mergeOrderedSeqs[E: Ordering]
  (a: Seq[E], b: Seq[E])
  (implicit o: Ordering[E]):
  Seq[E] = {
    @tailrec
    def rec(x: Seq[E], y: Seq[E], acc: Builder[E, Seq[E]]): Builder[E, Seq[E]] = {
      (x, y) match {
        case (Nil, Nil) => acc
        case (_, Nil) => acc ++= x
        case (Nil, _) => acc ++= y
        case (xh +: xt, yh +: yt) =>
          if (o.lteq(xh, yh))
            rec(xt, y, acc += xh)
          else
            rec(x, yt, acc += yh)
      }
    }
    // Use an ArrayBuffer set to the correct capacity as a Builder
    rec(a, b, new ArrayBuffer(a.length + b.length)).result
  }

  /** Merge a sequence composed of entries containing Eithers.
    *
    * Entries of Eithers of a same kind (Left or Right) cannot overlap.
    *
    * Overlapping entries will be split where necessary and their values passed to the
    * operator to be merged. Left and Right entries are passed as the first and second argument
    * of the merge operator, respectively.
    */
  @tailrec
  def mergeEithers[A, B, C]
  (done: Seq[TSEntry[C]]) //
  (todo: Seq[TSEntry[Either[A, B]]])
  (op: (Option[A], Option[B]) => Option[C])
  : Seq[TSEntry[C]] =
    todo match {
      case Seq() => // Nothing remaining, we are done -> return the merged Seq
        done
      case Seq(head, remaining@_*) =>
        // Take the head and all entries with which it overlaps and merge them.
        // Remaining entries are merged via a recursive call
        val (toMerge, nextRound) =
          remaining.span(_.timestamp < head.definedUntil()) match {
            case (vals :+ last, r) if last.defined(head.definedUntil) =>
              // we need to add the part that is defined after the head to the 'nextRound' entries
              (vals :+ last, last.trimEntryLeft(head.definedUntil) +: r)
            case t: Any => t
          }
        // Check if there was some empty space between the last 'done' entry and the first remaining
        val filling = done.lastOption match {
          case Some(TSEntry(ts, valE, d)) =>
            if (ts + d == head.timestamp) // Continuous domain, no filling to do
              Seq.empty
            else
              op(None, None).map(TSEntry(ts + d, _, head.timestamp)).toSeq
          case _ => Seq.empty
        }
        val p = TSEntry.mergeSingleToMultiple(head, toMerge)(op)
        // Add the freshly merged entries to the previously done ones, call to self with the remaining entries.
        mergeEithers(done ++ filling ++ TSEntry.mergeSingleToMultiple(head, toMerge)(op))(nextRound)(op)
    }

  /**
    * This is needed to be able to pattern match on Vectors:
    * https://stackoverflow.com/questions/10199171/matcherror-when-match-receives-an-indexedseq-but-not-a-linearseq
    */
  object +: {

    def unapply[T](s: Seq[T]) =
      s.headOption.map(head => (head, s.tail))
  }

}
