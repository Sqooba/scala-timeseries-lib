package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.validation.TSEntryFitter

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.compat._

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
    */
  def mergeEntries[A, B, C](a: Seq[TSEntry[A]])(b: Seq[TSEntry[B]])(
      op: (Option[A], Option[B]) => Option[C],
      compress: Boolean = true
  ): Seq[TSEntry[C]] = {
    def mergedEntries: Seq[TSEntry[C]] =
      mergeEitherSeq(
        mergeOrderedSeqs(a.map(_.toLeftEntry[B]), b.map(_.toRightEntry[A]))
      )(op)

    if (compress) TSEntryFitter.validateEntries(mergedEntries, compress)
    else mergedEntries
  }

  /**
    * Combine two Seq's that are known to be ordered and return a Seq that is
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

  /** Merge a sequence composed of entries containing Eithers.
    *
    * Entries of Eithers of a same kind (Left or Right) cannot overlap.
    *
    * Overlapping entries will be split where necessary and their values passed to the
    * operator to be merged. Left and Right entries are passed as the first and second argument
    * of the merge operator, respectively.
    */
  private def mergeEitherSeq[A, B, C](in: Seq[TSEntry[Either[A, B]]])(
      op: (Option[A], Option[B]) => Option[C]
  ): Seq[TSEntry[C]] = {

    @tailrec def rec(
        remaining: Seq[TSEntry[Either[A, B]]],
        lastSeenDefinedUntil: Long,
        builder: mutable.Builder[TSEntry[C], Seq[TSEntry[C]]]
    ): Seq[TSEntry[C]] = remaining match {
      case Seq() => builder.result()

      // Fill the hole when neither of the two time-series were defined over a given domain
      case head +: _ if lastSeenDefinedUntil < head.timestamp =>
        applyEmptyMerge(lastSeenDefinedUntil, head.timestamp)(op).foreach(builder += _)

        rec(remaining, head.timestamp, builder)

      case head +: tail =>
        // Take all entries with which the head overlaps and merge them
        val (toMerge, newTail) = tail.span(_.timestamp < head.definedUntil)

        val newHead = toMerge.lastOption
        // and if that last entry is still defined after the head's domain
          .filter(_.defined(head.definedUntil))
          // it is split and the second part is reused in the next iteration
          .map(_.trimEntryLeft(head.definedUntil))
          .toList

        rec(newHead ++ newTail, head.definedUntil, builder ++= mergeSingleToMultiple(head, toMerge)(op))
    }

    rec(in, Long.MaxValue, Seq.newBuilder)
  }

  /** Merge the 'single' TSEntry to the 'others'.
    * The domain of definition of the 'single' entry is used:
    * non-overlapping parts of the other entries will not be merged.
    */
  private[timeseries] def mergeSingleToMultiple[A, B, R](
      single: TSEntry[Either[A, B]],
      others: Seq[TSEntry[Either[A, B]]]
  )(op: (Option[A], Option[B]) => Option[R]): Seq[TSEntry[R]] =
    others.collect {
      // Retain only entries overlapping with 'single', and constrain them to the 'single' domain.
      case entry if single.overlaps(entry) =>
        entry.trimEntryLeftNRight(single.timestamp, single.definedUntil)
    } match {
      // Merge remaining constrained entries
      case Seq() => mergeEitherToNone(single)(op).toSeq

      case Seq(alone) => mergeEithers(single, alone)(op)

      case toMerge =>
        // Take care of the potentially undefined domain before the 'others'
        mergeDefinedEmptyDomain(single)(single.timestamp, toMerge.head.timestamp)(op) ++
          // Merge the others to the single entry, including potential
          // undefined spaces between them.
          // Group by pairs of entries to be able to trim the single
          // one to the relevant domain for the individual merges
          toMerge
            .sliding(2)
            .flatMap[TSEntry[R]](
              p => mergeEithers(single.trimEntryLeftNRight(p.head.timestamp, p.last.timestamp), p.head)(op)
            ) ++
          // Take care of the last entry and the potentially undefined domain
          // after it and the end of the single one.
          mergeEithers(toMerge.last, single.trimEntryLeft(toMerge.last.timestamp))(op)
    }

  /** Merge the provided entry to a None on the domain spawned by [from, until[ */
  private def mergeDefinedEmptyDomain[A, B, R](e: TSEntry[Either[A, B]])(from: Long, until: Long)(op: (Option[A], Option[B]) => Option[R]): Seq[TSEntry[R]] =
    if (from == until) {
      Seq.empty
    } else {
      mergeEitherToNone(e.trimEntryLeftNRight(from, until))(op).toSeq
    }

  private[timeseries] def applyEmptyMerge[A, B, R](from: Long, to: Long)(op: (Option[A], Option[B]) => Option[R]): Option[TSEntry[R]] =
    if (from == to) None
    else op(None, None).map(TSEntry(from, _, to - from))

  /** Merge two TSEntries each containing an Either[A,B]
    * Simply calls the normal merge function after determining which of
    * the entries contains what type. */
  private[timeseries] def mergeEithers[A, B, R](a: TSEntry[Either[A, B]], b: TSEntry[Either[A, B]])(op: (Option[A], Option[B]) => Option[R]): Seq[TSEntry[R]] =
    (a, b) match {
      case (TSEntry(tsA, Left(valA), dA), TSEntry(tsB, Right(valB), dB)) =>
        TSEntry.merge(TSEntry(tsA, valA, dA), TSEntry(tsB, valB, dB))(op)
      case (TSEntry(tsB, Right(valB), dB), TSEntry(tsA, Left(valA), dA)) =>
        TSEntry.merge(TSEntry(tsA, valA, dA), TSEntry(tsB, valB, dB))(op)
      case _ =>
        throw new IllegalArgumentException(s"Can't pass two entries with same sided-eithers: $a, $b")
    }

  /** Merge the provided entry to a None, using the specified operator */
  private[timeseries] def mergeEitherToNone[A, B, R](e: TSEntry[Either[A, B]])(op: (Option[A], Option[B]) => Option[R]): Option[TSEntry[R]] = {
    e match {
      case TSEntry(_, Left(valA), _)  => op(Some(valA), None)
      case TSEntry(_, Right(valB), _) => op(None, Some(valB))
    }
  }.map(TSEntry(e.timestamp, _, e.validity))

}
