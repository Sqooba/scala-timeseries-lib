package io.sqooba.oss.timeseries

import java.util.concurrent.TimeUnit

import io.sqooba.oss.timeseries.immutable._
import io.sqooba.oss.timeseries.archive.TimeBucketer

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Builder}
import scala.concurrent.duration.TimeUnit
import scala.reflect.runtime.universe._

trait TimeSeries[+T] {

  /** The value valid at time 't' if there is one. */
  def at(t: Long): Option[T]

  /** The whole entry containing the value valid at time 't', if there is one */
  def entryAt(t: Long): Option[TSEntry[T]]

  /** Split this time series into two.
    *
    * Returns a tuple of two contiguous time series,
    * such that the left time series is never defined for t >= 'at'
    * and the right time series is never defined for t < 'at'.
    *
    * Default implementation simply returns (this.trimRight(at), this.trimLeft(at))
    */
  def split(at: Long): (TimeSeries[T], TimeSeries[T]) = (this.trimRight(at), this.trimLeft(at))

  /**
    * Split this time series into two.
    *
    * If 'at' is within the domain of a particular entry, the time series will be split either
    * at the beginning or end of that entry, according to the passed 'splitAfterEntry'
    */
  def splitDiscrete(at: Long, splitAfterEntry: Boolean = true): (TimeSeries[T], TimeSeries[T]) =
    (this.trimRightDiscrete(at, splitAfterEntry), this.trimLeftDiscrete(at, !splitAfterEntry))

  /** Extract a slice from this time series.
    *
    * The returned slice will only be defined between the specified bounds such that:
    *
    *  this.at(x) == returned.at(x) for all x in [from, to[.
    *
    * If x is outside of the bounds, returned.at(x) is None.
    */
  def slice(from: Long, to: Long): TimeSeries[T] = this.trimLeft(from).trimRight(to)

  /**
    * Extract a slice from this time series, while preventing entries on the slice boundaries
    * from being split.
    */
  def sliceDiscrete(from: Long, to: Long, fromInclusive: Boolean = true, toInclusive: Boolean = true): TimeSeries[T] =
    this.trimLeftDiscrete(from, fromInclusive).trimRightDiscrete(to, toInclusive)

  /** Returns a time series that is never defined for t >= at and unchanged for t < at */
  def trimRight(at: Long): TimeSeries[T]

  /** Similar to trimRight, but if `at` lands within an existing entry,
    * the returned time series' domain is either, depending on 'includeEntry':
    *  - extended to that entry's end of validity, which fully remains in the time series (the default)
    *  - trimmed further to the previous entry's end of validity, fully removing the entry from the time series
    */
  def trimRightDiscrete(at: Long, includeEntry: Boolean = true): TimeSeries[T]

  /** Returns a time series that is never defined for t < at and unchanged for t >= at */
  def trimLeft(at: Long): TimeSeries[T]

  /** Similar to trimLeft, but if `at` lands within an existing entry,
    * the returned time series' domain is either, depending on 'includeEntry':
    *  - extended to that entry's timestamp, which fully remains in the time series (the default)
    *  - trimmed further to the next entry's timestamp, fully removing the entry from the time series
    */
  def trimLeftDiscrete(at: Long, includeEntry: Boolean = true): TimeSeries[T]

  /** The number of elements in this time-series. */
  def size: Int

  /** Convenient and efficient method for `size == 0` */
  def isEmpty: Boolean

  /** Convenient method for !isEmpty */
  def nonEmpty: Boolean = !isEmpty

  /** True if this time series is defined at 'at'. Ie, at('at') would return Some[T] */
  def defined(t: Long): Boolean = at(t).isDefined

  /** Returns true if this timeseries was compressed at some point in its construction. */
  def isCompressed: Boolean

  /** Map the values within the time series.
    * the 'compress' parameters allows callers to control whether or not compression should occur.
    * If set to false, timestamps and validities remain unchanged. Defaults to true */
  def map[O: WeakTypeTag](f: T => O, compress: Boolean = true): TimeSeries[O]

  /** Map the values within the time series.
    * Timestamps and validities of entries remain unchanged,
    * but the time is made available for cases where the new value would depend on it. */
  def mapWithTime[O: WeakTypeTag](f: (Long, T) => O, compress: Boolean = true): TimeSeries[O]

  /** Return a time series that will only contain entries for which the passed predicate returned True. */
  def filter(predicate: TSEntry[T] => Boolean): TimeSeries[T]

  /** Return a time series that will only contain entries containing values for which the passed predicate returned True. */
  def filterValues(predicate: T => Boolean): TimeSeries[T]

  /** Fill the wholes in the definition domain of this time series with the passed value.
    * The resulting time series will have a single continuous definition domain,
    * provided the original time series was non-empty. */
  def fill[U >: T](whenUndef: U): TimeSeries[U]

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

  /** Return the first (chronological) value in this time series.
    *
    * @throws NoSuchElementException if this time series is empty. */
  def headValue: T = head.value

  /** Return a filled option containing the first (chronological) value in this
    * time series.
    * None if this time series is empty. */
  def headValueOption: Option[T] = headOption.map(_.value)

  /** Return the last (chronological) entry in this time series.
    *
    * @throws NoSuchElementException if this time series is empty. */
  def last: TSEntry[T]

  /** Return a filled option containing the last (chronological) entry in this
    * time series.
    * None if this time series is empty. */
  def lastOption: Option[TSEntry[T]]

  /** Return the last (chronological) value in this time series.
    *
    * @throws NoSuchElementException if this time series is empty. */
  def lastValue: T = last.value

  /** Return a filled option containing the last (chronological) value in this time series.
    * None if this time series is empty. */
  def lastValueOption: Option[T] = lastOption.map(_.value)

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
  def append[U >: T](other: TimeSeries[U], compress: Boolean = true): TimeSeries[U] =
    other.headOption
      .map(head => this.trimRight(head.timestamp).entries ++ other.entries)
      .map(_.foldLeft(newBuilder[U](compress))(_ += _).result())
      .getOrElse(this)

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
  def prepend[U >: T](other: TimeSeries[U], compress: Boolean = true): TimeSeries[U] =
    other.lastOption
      .map(last => other.entries ++ this.trimLeft(last.definedUntil).entries)
      .map(_.foldLeft(newBuilder[U](compress))(_ += _).result())
      .getOrElse(this)

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
  // TODO make merge logic streamable. As the this.entries are called here, this creates a Seq[TSEntry] which means
  //      that the benefits of other implementations vanish.
  def merge[O, R](op: (Option[T], Option[O]) => Option[R])(other: TimeSeries[O]): TimeSeries[R] =
    TimeSeries.ofOrderedEntriesUnsafe(TimeSeries.mergeEntries(this.entries)(other.entries)(op))

  /**
    * Merge another time series to this one, using the provided operator
    * to merge entries. The resulting series has the value defined by the operator
    * at all times where both input series are defined. At all other times (i.e. where only one
    * or none of the input series is defined), the resulting series is not defined.
    *
    * @param op    the operator for the merge
    * @param other TimeSeries to merge
    * @return the strictly merged TimeSeries
    */
  def strictMerge[O, R](op: (T, O) => R)(other: TimeSeries[O]): TimeSeries[R] =
    this.merge[O, R] {
      case (Some(t), Some(o)) => Some(op(t, o))
      case _                  => None
    }(other)

  /**
    * Sum the entries within this and the provided time series such that
    *
    * - If strict (default): this.at(x) + other.at(x) = returned.at(x) where x may take any value where
    * both time series are defined.
    * - If non strict : this.at(x) + other.at(x) = returned.at(x) where x may take any value where
    * any time series is defined.
    */
  def plus[U >: T](other: TimeSeries[U], strict: Boolean = true)(implicit n: Numeric[U]): TimeSeries[U] = {
    import n._

    if (strict) {
      strictMerge[U, U](_ + _)(other)
    } else {
      merge[U, U](NumericTimeSeries.nonStrictPlus(_, _)(n))(other)
    }
  }

  def +[U >: T](other: TimeSeries[U])(implicit n: Numeric[U]): TimeSeries[U] = plus(other)(n)

  /**
    * Subtract the entries within this and the provided time series such that
    * this.at(x) - other.at(x) = returned.at(x) where x may take any value where
    * both time series are defined.
    */
  def minus[U >: T](
      other: TimeSeries[U],
      leftHandDefault: Option[U] = None,
      rightHandDefault: Option[U] = None
  )(implicit n: Numeric[U]): TimeSeries[U] =
    merge[U, U](NumericTimeSeries.nonStrictMinus(leftHandDefault, rightHandDefault)(_, _)(n))(other)

  def -[U >: T](other: TimeSeries[U])(implicit n: Numeric[U]): TimeSeries[U] = minus(other)

  /**
    * Multiply the entries within this and the provided time series such that
    * this.at(x) * other.at(x) = returned.at(x) where x may take any value where
    * both time series are defined.
    */
  def multiply[U >: T](other: TimeSeries[U])(implicit n: Numeric[U]): TimeSeries[U] = {
    import n._

    strictMerge[U, U](_ * _)(other)
  }

  def *[U >: T](other: TimeSeries[U])(implicit n: Numeric[U]): TimeSeries[U] = multiply(other)

  /**
    * Zips this time series with another one, returning a time series
    * of tuples containing the values from both this and the other time
    * series across their common domain.
    */
  def strictZip[O](other: TimeSeries[O]): TimeSeries[(T, O)] = strictMerge[O, (T, O)]((_, _))(other)

  /**
    * Computes the integral of this time series.
    * This function returns a step function, so only represents an approximation.
    * Use it if you need to compute multiple integrals of the same time series.
    */
  def stepIntegral[U >: T](stepLengthMs: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS)(implicit n: Numeric[U]): TimeSeries[Double] = {
    TimeSeries.ofOrderedEntriesUnsafe(
      NumericTimeSeries.stepIntegral[U](
        this.splitEntriesLongerThan(stepLengthMs).entries,
        timeUnit
      )
    )
  }

  /**
    * Compute the integral of this time series between the two specified points.
    * Simply sums values on the [from, to] interval.
    * If you need to call this multiple times, consider using #stepIntegral()
    * depending on your use case.
    */
  def integrateBetween[U >: T](from: Long, to: Long)(implicit n: Numeric[U]): U =
    this.slice(from, to).entries.map(_.value).sum(n)

  /**
    * Splits up all entries of this timeseries that are longer than the given maximal length.
    * This is slightly similar but not strictly equivalent to resampling a series:
    * if you need some form of resampling, make sure this is what you need.
    *
    * @param entryMaxLength to use for splitting
    * @return timeseries with entries guaranteed to be shorter than the given
    *         maximal length
    */
  def splitEntriesLongerThan(entryMaxLength: Long): TimeSeries[T] =
    TimeSeries
      .splitEntriesLongerThan(entries, entryMaxLength)
      // we don't want to compress the consecutive equal entries that result from
      // splitting a long entry up
      .foldLeft(newBuilder[T](compress = false))(_ += _)
      .result()

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
  def slidingIntegral[U >: T](
      window: Long,
      timeUnit: TimeUnit = TimeUnit.MILLISECONDS
  )(implicit n: Numeric[U]): TimeSeries[Double] =
    if (this.size < 2) {
      this.map(n.toDouble)
    } else {
      // TODO: have slidingSum return compressed output so we can use the unsafe constructor
      //   and save an iteration
      NumericTimeSeries
        .slidingIntegral[U](this.entries, window, timeUnit)
        .foldLeft(newBuilder[Double]())(_ += _)
        .result()
    }

  /**
    * Buckets this TimeSeries into sub-time-series that have a domain of definition that is at most that
    * specified by the passed bucket boundaries.
    *
    * @param buckets a stream of times representing bucket boundaries. A stream of (a, b, c, ...)
    *                will generate buckets with domain (([a, b[), ([b, c[), ...)
    *                Note that it is wise to have 'buckets' start at a meaningfully close point in time
    *                relative to the time-series first entry.
    * @return a stream of (bucket-start, timeseries).
    */
  def bucket(buckets: Stream[Long]): Stream[(Long, TimeSeries[T])] =
    TimeBucketer.bucketEntriesToTimeSeries(buckets, this.entries, newBuilder[T]())

  /**
    * Returns the bounds of the domain
    *
    * If the time-series does not contain any "hole" in its domain, then the loose domain is equal to
    * its domain. Otherwise, the loose domain only contains the min/max bounds of the domain.
    *
    * Said otherwise, the time-series is guaranteed to be undefined outside of the loose domain, and
    * has at least a point where it is defined within the loose domain.
    *
    * @return The oldest and newest timestamps where the time-series is defined, encapsulated in a `LooseDomain`
    */
  def looseDomain: TimeDomain

  /**
    * Fallback to `other` when `this` is not defined
    *
    * @param other Another time-series which should contain the value when `this` is not defined
    * @tparam U The new underlying parameter
    * @return A time-series which contains the values of `this` if defined, and of `other` otherwise
    */
  def fallback[U >: T](other: TimeSeries[U]): TimeSeries[U] =
    merge[U, U] {
      case (Some(v), _)    => Some(v)
      case (_, otherValue) => otherValue
    }(other)

  /**
    * @return The probability that the time-series is defined over its loose-domain
    */
  def supportRatio: Double

  /**
    *
    * @return whether the TimeSeries is defined for all t in its looseDomain.
    *         I. e. whether there are holes in its time domain or not.
    */
  def isDomainContinuous: Boolean

  /**
    * @return a builder that constructs a new timeseries of this implementation
    */
  def newBuilder[U](compress: Boolean = true)(implicit tag: WeakTypeTag[U]): TimeSeriesBuilder[U] =
    TimeSeries.newBuilder(compress)
}

object TimeSeries {

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
      in
    } else {
      fillMe(in, fillValue, Seq.newBuilder[TSEntry[T]])
    }

  @tailrec
  private def fillMe[T](in: Seq[TSEntry[T]], fillValue: T, acc: mutable.Builder[TSEntry[T], Seq[TSEntry[T]]]): Seq[TSEntry[T]] =
    in match {
      case Seq(first, last) =>
        // Only two elements remaining: the recursion can end
        (acc ++= fillAndCompress(first, last, fillValue)).result()
      case Seq(first, second, tail @ _*) =>
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

  /** Returns a Sequence of entries such that there is no discontinuity
    * between current.timestamp and next.definedUntil, filling the gap
    * between the entries and compression them if necessary. */
  def fillAndCompress[T](first: TSEntry[T], second: TSEntry[T], fillValue: T): Seq[TSEntry[T]] = {
    if (first.definedUntil == second.timestamp) {
      // Entries contiguous.
      Seq(first, second)
    } else {
      // There is space to fill
      first.appendEntry(
        TSEntry(first.definedUntil, fillValue, second.timestamp - first.definedUntil)
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
  def mergeEntries[A, B, C](a: Seq[TSEntry[A]])(b: Seq[TSEntry[B]])(op: (Option[A], Option[B]) => Option[C]): Seq[TSEntry[C]] =
    mergeEithers(
      mergeOrderedSeqs(a.map(_.toLeftEntry[B]), b.map(_.toRightEntry[A]))
    )(op)

  /**
    * Combine two Seq's that are known to be ordered and return a Seq that is
    * both ordered and that contains both of the elements in 'a' and 'b'.
    * Adapted from http://stackoverflow.com/a/19452304/1997056
    */
  def mergeOrderedSeqs[E](a: Seq[E], b: Seq[E])(implicit o: Ordering[E]): Seq[E] = {
    @tailrec
    def rec(x: Seq[E], y: Seq[E], acc: mutable.Builder[E, Seq[E]]): mutable.Builder[E, Seq[E]] = {
      (x, y) match {
        case (Nil, Nil) => acc
        case (_, Nil)   => acc ++= x
        case (Nil, _)   => acc ++= y
        case (xh +: xt, yh +: yt) =>
          if (o.lteq(xh, yh)) {
            rec(xt, y, acc += xh)
          } else {
            rec(x, yt, acc += yh)
          }
      }
    }
    // Use an ArrayBuffer set to the correct capacity as a Builder
    rec(a, b, Seq.newBuilder).result
  }

  /** Merge a sequence composed of entries containing Eithers.
    *
    * Entries of Eithers of a same kind (Left or Right) cannot overlap.
    *
    * Overlapping entries will be split where necessary and their values passed to the
    * operator to be merged. Left and Right entries are passed as the first and second argument
    * of the merge operator, respectively.
    */

  // TODO make merge logic streamable. Currently, it uses the default builder which will create a VectorTimeSeries.
  def mergeEithers[A, B, C](in: Seq[TSEntry[Either[A, B]]])(op: (Option[A], Option[B]) => Option[C]): Seq[TSEntry[C]] = {

    // Holds the final merged list
    val result = newBuilder[C]()
    // Holds the current state of the entries to process
    // TODO looks like Stack is deprecated: see if we can use a List instead
    val current = mutable.Stack[TSEntry[Either[A, B]]](in: _*)

    var lastSeenDefinedUntil: Long = Long.MaxValue

    while (current.nonEmpty) {
      // entries that need to be merged
      val toMerge = new ArrayBuffer[TSEntry[Either[A, B]]]()
      val head    = current.pop()

      // Fill the hole when neither of the two time-series were defined over a given domain
      // In order to do se, we re-use the `definedUntil` property of the last TSentry seen
      if (lastSeenDefinedUntil < head.timestamp) {
        val nTimestamp = lastSeenDefinedUntil
        op(None, None).map(TSEntry(nTimestamp, _, head.timestamp - nTimestamp)).foreach(result += _)

        lastSeenDefinedUntil = head.timestamp

        // We didn't process the head, so we should re-stack it
        current.push(head)
      } else {

        // Take the head and all entries with which it overlaps and merge them.
        while (current.nonEmpty && current.head.timestamp < head.definedUntil) {
          toMerge.append(current.pop())
        }

        // If the last entry to merge is defined after the head,
        // it is split and added back to the list
        // of entries to process
        if (toMerge.nonEmpty && toMerge.last.defined(head.definedUntil)) {
          current.push(toMerge.last.trimEntryLeft(head.definedUntil))
        }

        // Check if there was some empty space between the last 'done' entry and the first remaining
        val filling = result.definedUntil match {
          case Some(definedUntil) =>
            if (definedUntil == head.timestamp) { // Continuous domain, no filling to do
              Seq.empty
            } else {
              op(None, None).map(TSEntry(definedUntil, _, head.timestamp - definedUntil)).toSeq
            }
          case _ => Seq.empty
        }

        lastSeenDefinedUntil = head.definedUntil

        val p = TSEntry.mergeSingleToMultiple(head, toMerge.toSeq)(op)
        result ++= filling
        result ++= p
      }
    }

    result.result().entries
  }

  /**
    * Groups the entries in the stream into substreams that each contain at most
    * maxNumberOfEntries.
    *
    * @param entries as a stream
    * @param maxNumberOfEntries contained by each substream of the result
    * @return a stream of (bucket-start, bucket-entries)
    */
  def groupEntries[T](
      entries: Stream[TSEntry[T]],
      maxNumberOfEntries: Int
  ): Stream[(Long, Stream[TSEntry[T]])] =
    entries
      .grouped(maxNumberOfEntries)
      .toStream
      .map(substream => (substream.head.timestamp, substream))

  /**
    * Splits up all entries in the input that are longer than the given maximal length.
    * @param entryMaxLength to use for splitting
    * @return a sequence of entries guaranteed to be shorter than the given
    *         maximal length
    */
  def splitEntriesLongerThan[T](entries: Seq[TSEntry[T]], entryMaxLength: Long): Seq[TSEntry[T]] =
    entries.flatMap(entry => entry.splitEntriesLongerThan(entryMaxLength).entries)

  /**
    * This is needed to be able to pattern match on Vectors:
    * https://stackoverflow.com/questions/10199171/matcherror-when-match-receives-an-indexedseq-but-not-a-linearseq
    */
  // scalastyle:off object_name
  object +: {

    def unapply[T](s: Seq[T]): Option[(T, Seq[T])] =
      s.headOption.map(head => (head, s.tail))
  }

  // scalastyle:on object_name

  /**
    * Computes the union of the passed time-series' loose domains
    *
    * @param tss A sequence of time-series
    * @tparam T The underlying type of the time-series
    * @return The union of the LooseDomains
    */
  def unionLooseDomains[T](tss: Seq[TimeSeries[T]]): TimeDomain =
    tss.map(_.looseDomain).fold(EmptyTimeDomain)(_.looseUnion(_))

  /**
    * Computes the intersection of the passed time-series' loose domains
    *
    * @note If there is an empty time-series, then the intersection will be None.
    * @param tss A sequence of time-series
    * @tparam T The underlying type of the time-series
    * @return The intersection of the LooseDomains
    */
  def intersectLooseDomains[T](tss: Seq[TimeSeries[T]]): TimeDomain =
    if (tss.isEmpty) {
      EmptyTimeDomain
    } else {
      tss.map(_.looseDomain).reduce(_.intersect(_))
    }

  /**
    * Construct a time-series using an ordered Seq of TSEntries
    *
    * The correct underlying implementation will be used depending of the Seq's size
    * (i.e. EmptyTimeSeries, TSEntry, or a VectorTimeSeries)
    *
    * @note The sequence has to be chronologically ordered, otherwise the time-series might
    *       not behave correctly. In general, you should use a `TimeSeries.newBuilder`. Furthermore, no
    *       two entries should have the same timestamp. Finally, entries will NOT be compressed.
    * @param xs A sequence of TSEntries which HAS to be chronologically ordered (w.r.t. their timestamps) and
    *           well-formed (no duplicated timestamps)
    * @param isCompressed Flags whether the xs' have been compressed in their construction.
    *                     Will be passed to the underlying implementation.
    * @param isDomainContinous Flags whether all the entries span a continuous time domain without holes.
    *                          Will be passed to the underlying implementation.
    * @tparam T The underlying type of the time-series
    * @return A time-series with a correct implementation
    */
  def ofOrderedEntriesUnsafe[T](
      xs: Seq[TSEntry[T]],
      isCompressed: Boolean = false,
      isDomainContinous: Boolean = false
  ): TimeSeries[T] = {
    val size = xs.size

    if (size == 0) {
      EmptyTimeSeries
    } else if (size == 1) {
      xs.head
    } else {
      VectorTimeSeries.ofOrderedEntriesUnsafe(xs, isCompressed, isDomainContinous)
    }
  }

  /**
    * Construct using a time-series `TimeSeriesBuilder` given an ordered list of entries
    *
    * The correct underlying implementation will be chosen (EmptyTimeSeries, TSEntry or VectorTimeSeries).
    * As we are using a `TimeSeriesBuilder`, the entries will be compressed if possible.
    *
    * @note No two entries can have the same timestamp, an exception will be thrown if it's the case.
    * @param xs A sequence of TSEntries which HAS to be chronologically ordered (w.r.t. their timestamps) and
    *           well-formed (no duplicated timestamps)
    * @param compress A flag specifying whether the entries should be compressed or not.
    * @tparam T The underlying type of the time-series
    * @return A compressed time-series with a correct implementation
    */
  def ofOrderedEntriesSafe[T](xs: Seq[TSEntry[T]], compress: Boolean = true): TimeSeries[T] =
    xs.foldLeft(newBuilder[T](compress))(_ += _).result()

  /**
    * An safe constructor of `TimeSeries`
    *
    * The given entries are sorted, compressed (if needed) and returned as a time-series. If the sequence
    * is empty, then it returns an `EmptyTimeSeries`. If the sequence is made of only one entry, then it returns
    * it.
    *
    * @note All entries should have different timestamps.
    * @param entries A sequence of entries which all have a different timestamp
    * @tparam T The time-series' underlying parameter
    * @return A well initialized time-series
    */
  def apply[T](entries: Seq[TSEntry[T]]): TimeSeries[T] = ofOrderedEntriesSafe(entries.sorted)

  /**
    * @return the default timeseries builder implementation
    */
  def newBuilder[T](compress: Boolean = true): TimeSeriesBuilder[T] = new VectorTimeSeries.Builder[T](compress)
}
