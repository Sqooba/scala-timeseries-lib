package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.validation.TSEntryFitter

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.compat._
import scala.language.reflectiveCalls

object TimeSeriesMerger {

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
    *
    * @param compress specifies whether the entries should be validated and compressed
    *                 by a TSEntryFitter. This should only be omitted if the entries
    *                 will be passed to a TimeSeriesBuilder subsequently.
    */
  def mergeEntries[A, B, R](a: Seq[TSEntry[A]], b: Seq[TSEntry[B]], compress: Boolean = true)(
      op: (Option[A], Option[B]) => Option[R]
  ): Seq[TSEntry[R]] =
    mergeEntries[(Option[A], Option[B]), R](
      mergeOrderedSeqs(
        a.map(_.map(av => (Some(av), None))),
        b.map(_.map(bv => (None, Some(bv))))
      ),
      op.tupled,
      (None, None),
      (pair1, pair2) => (pair1._1 orElse pair2._1, pair1._2 orElse pair2._2),
      compress
    )

  /** See [[mergeEntries]]. The same as mergeEntries for two sequences but for three. */
  def mergeEntries[A, B, C, R](a: Seq[TSEntry[A]], b: Seq[TSEntry[B]], c: Seq[TSEntry[C]], compress: Boolean)(
      op: (Option[A], Option[B], Option[C]) => Option[R]
  ): Seq[TSEntry[R]] =
    mergeEntries[(Option[A], Option[B], Option[C]), R](
      mergeOrderedSeqs(
        a.map(_.map(av => (Some(av), None, None))),
        mergeOrderedSeqs(
          b.map(_.map(bv => (None, Some(bv), None))),
          c.map(_.map(cv => (None, None, Some(cv))))
        )
      ),
      op.tupled,
      (None, None, None),
      (pair1, pair2) => (pair1._1 orElse pair2._1, pair1._2 orElse pair2._2, pair1._3 orElse pair2._3),
      compress
    )

  /** Takes a sequence of entries that are possibly overlapping in time and returns a
    * sequence where all overlapping entries have first been reduced to a single
    * value (`reduceOp`) and then merged to the new type (`mergeOp`).
    *
    * @note Because this function is agnostic of the type it can (as an example) work
    *       on tuples of any length. This allows us to encode merges of any arity with the
    *       following tuple encoding:
    *
    *       The tuples have a length equal to the number of TimeSeries' to merge.
    *       Each tuple corresponds to one original TSEntry. Example: if there are
    *       three TimeSeries to merge `tsA`, `tsB` and `tsC` then a `TSEntry(ts, v, d)`
    *       of `tsC` will be converted to an entry of the following form:
    *       {{{
    *         TSEntry(ts, (None, None, Some(v)), d)
    *       }}}
    *
    *       Overlapping entries from different TimeSeries will be split where
    *       necessary and, subsequently, all entries covering the same time will be
    *       reduced to single one where more than one position of the tuple has a
    *       Some. For example:
    *       {{{
    *         TSEntry(ts, (Some(va), None, Some(vc)), d)
    *       }}}
    *       means that at the time `ts` until `ts + d` the `tsA` has value `va` and
    *       the `tsC` has value `vc`. The contained tuple now corresponds to the
    *       argument of the `mergeOp`; it can simply be applied.
    *
    *
    * @param in       sequence of entries that can overlap in time by are strictly ordered by timestamp
    * @param mergeOp  operator from the entry type to the final type or None
    * @param allNone  the value to use for the merge operator when no entry is defined
    * @param reduceOp reduces overlapping entry-values to a single one
    * @param compress specifies whether the entries should be validated and compressed
    *                 by a TSEntryFitter. This should only be omitted if the entries
    *                 will be passed to a TimeSeriesBuilder subsequently.
    * @return the sequence of merged but not validated/compressed TSEntries of the result type
    */
  def mergeEntries[T, R](
      in: => Seq[TSEntry[T]],
      mergeOp: T => Option[R],
      allNone: T,
      reduceOp: (T, T) => T,
      compress: Boolean
  ): Seq[TSEntry[R]] = {
    // def instead of val to guarantee laziness
    def mergedEntries =
      mergeEitherSeq(in, mergeOp, allNone, reduceOp)

    if (compress) TSEntryFitter.validateEntries(mergedEntries, compress)
    else mergedEntries
  }

  /** Combine two Seq's that are known to be ordered and return a Seq that is
    * both ordered and that contains both of the elements in 'a' and 'b'.
    * Adapted from http://stackoverflow.com/a/19452304/1997056
    */
  private[timeseries] def mergeOrderedSeqs[E](a: Seq[E], b: Seq[E])(implicit o: Ordering[E]): Seq[E] = {
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
    rec(a, b, Seq.newBuilder).result
  }

  /** See the general [[mergeEntries]] for explanation of the algorithm. */
  private def mergeEitherSeq[T, C](
      in: Seq[TSEntry[T]],
      op: T => Option[C],
      allNone: T,
      mergeValues: (T, T) => T
  ): Seq[TSEntry[C]] = {
    // TODO: the implementation of this function introduced in commit: "[timeseries]
    //   Shapeless merge for pairs. a5e45f8962dff0c1f7" does support streaming by using
    //   LazyList. Revert to that implementation once Scala 2.12 is not needed anymore.

    @tailrec
    def rec(remaining: List[TSEntry[T]], lastSeenDefinedUntil: Long, builder: mutable.Builder[TSEntry[C], Seq[TSEntry[C]]]): Seq[TSEntry[C]] =
      remaining match {
        case Seq() => builder.result

        // Fill the hole when neither of the two time series were defined over a given domain
        case head +: _ if lastSeenDefinedUntil < head.timestamp =>
          rec(remaining, head.timestamp, builder ++= applyEmptyMerge(lastSeenDefinedUntil, head.timestamp)(allNone, op))

        case entries =>
          // Take all entries that start at the same time as head, including the head.
          val (startAtHead, newTail) = entries.span(_.timestamp == entries.head.timestamp)

          val closestDefinedUntil = startAtHead.map(_.definedUntil).min

          // The next cut occurs when any of the TimeSeries to merge changes value. The
          // cut is either the closest definedUntil of any of the entries that start at
          // the same time as the head. Or it is the beginning of the next closest
          // entry.
          val nextCutTs =
            if (newTail.isEmpty) closestDefinedUntil
            else Math.min(newTail.head.timestamp, closestDefinedUntil)

          val toMerge = startAtHead.map(_.trimEntryRight(nextCutTs))
          // Retain the entries that extend longer than the cut
          val newHeads = startAtHead.filter(_.definedUntil > nextCutTs).map(_.trimEntryLeft(nextCutTs))

          rec(newHeads ::: newTail, entries.head.definedUntil, builder ++= mergeSameDomain(toMerge, op, mergeValues))
      }

    rec(in.toList, Long.MaxValue, Seq.newBuilder)
  }

  /** Merge a sequence of entries of exactly the same domain. There must be at least
    * one entry and at most one per TimeSeries to merge, as they cannot overlap. The
    * entries' values are merged by the given operator
    *
    * @param entriesSameDomain to merge, at least one
    * @param op merge operator from HList to the result type or None
    * @return a TSEntry of the result type or None
    */
  private def mergeSameDomain[T, R](
      entriesSameDomain: Seq[TSEntry[T]],
      op: T => Option[R],
      mergeValues: (T, T) => T
  ): Option[TSEntry[R]] =
    entriesSameDomain match {
      // check that all entries have the same start timestamp and end
      case TSEntry(ts, _, d) +: others if others.forall(e => e.timestamp == ts && e.validity == d) =>
        op(entriesSameDomain.map(_.value).reduce(mergeValues)).map(TSEntry(ts, _, d))

      case _ =>
        throw new IllegalArgumentException("Can only merge a non-empty sequence of entries with exactly the same domain.")
    }

  /** Create a TSEntry with the empty value's result or None. */
  private def applyEmptyMerge[T, R](from: Long, to: Long)(
      allNone: T,
      op: T => Option[R]
  ): Option[TSEntry[R]] =
    if (from >= to) None
    else op(allNone).map(TSEntry(from, _, to - from))

}
