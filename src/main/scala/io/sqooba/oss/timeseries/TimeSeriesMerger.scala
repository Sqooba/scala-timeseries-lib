package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.validation.TSEntryFitter

import shapeless._
import shapeless.ops.hlist._
import shapeless.UnaryTCConstraint.*->*
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.compat._
import scala.language.reflectiveCalls

// This is needed for the lambda symbol that is used in some type constraints.
// scalastyle:off non.ascii.character.disallowed

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
  def mergeEntries[A, B, C](a: Seq[TSEntry[A]], b: Seq[TSEntry[B]], compress: Boolean = true)(
      op: (Option[A], Option[B]) => Option[C]
  ): Seq[TSEntry[C]] = {
    type OptionHList = Option[A] :: Option[B] :: HNil

    // the given merge operator but taking an HList as an argument
    val hlistOp: OptionHList => Option[C] = cp => op tupled Generic[(Option[A], Option[B])].from(cp)

    // the result of the merge operator when all input TimeSeries are None
    val allNoneResult: Option[C] = hlistOp(allNoneHList(hListLength[OptionHList]))

    // def instead of val to guarantee laziness
    def mergedEntries: Seq[TSEntry[C]] =
      mergeEitherSeq(
        mergeOrderedSeqs(
          a.map(_.map(av => Some(av) :: None :: HNil)),
          b.map(_.map(bv => None :: Some(bv) :: HNil))
        )
        // IntelliJ unfortunately complains about implicits here but it is wrong – just ignore.
      )(hlistOp, allNoneResult)

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

  /** Merge a sequence of HLists of Options ordered by timestamp, with the given
    * operator.
    *
    * The HLists have a length equal to the number of TimeSeries' to merge. Each
    * HList corresponds to one original TSEntry. If there are three TimeSeries to
    * merge tsA, tsB and tsC then a TSEntry(ts, v, d) of tsC will be converted to an
    * entry of the following form:
    *
    * TSEntry(ts, None :: None :: Some(v) :: HNil, d)
    *
    * Overlapping entries from different TimeSeries will be split where necessary
    * and, subsequently, all entries covering the same time will be merged. For
    * example:
    *
    * TSEntry(ts, Some(va) :: None :: Some(vc) :: HNil, d)
    *
    * means that at the time 'ts' until 'ts + d' the tsA has value va and the tsC has
    * value vc. The contained HList now also corresponds to the argument of the
    * transformed merge operator that takes an HList as argument. It can simply be
    * applied.
    *
    * @param in sequence of HLists of Options ordered by timestamp
    * @param op merge operator from HList to the result type or None
    * @param allNone the value of the merge operator when passed an HList of only None
    * @return the sequence of merged but not validated/compressed TSEntries of the result type C
    */
  private def mergeEitherSeq[
      // an HList of Options
      T <: HList: *->*[Option]#λ,
      ZO <: HList,
      C
  ](in: Seq[TSEntry[T]])(op: T => Option[C], allNone: Option[C])(
      // implicits for mergeHLists
      implicit zipper: Zip.Aux[T :: T :: HNil, ZO],
      mapper: Mapper.Aux[Merge.type, ZO, T]
  ): Seq[TSEntry[C]] = {

    // Returns a lazily evaluated stream of the merged result entries.
    // Streaming is applied to all types of input sequences for performance reasons.
    def rec(remaining: Stream[TSEntry[T]], lastSeenDefinedUntil: Long): Stream[TSEntry[C]] = remaining match {
      case Seq() => Stream.empty

      // Fill the hole when neither of the two time-series were defined over a given domain
      case head +: _ if lastSeenDefinedUntil < head.timestamp =>
        applyEmptyMerge(lastSeenDefinedUntil, head.timestamp)(allNone).to(Stream) #::: rec(remaining, head.timestamp)

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

        mergeSameDomain(toMerge)(op).to(Stream) #::: rec(newHeads #::: newTail, entries.head.definedUntil)
    }

    rec(in.to(Stream), Long.MaxValue)
  }

  /**
    * Merge a sequence of entries of exactly the same domain. There must be at least
    * one entry and at most one per TimeSeries to merge, as they cannot overlap. The
    * entries are first merged to a single HList and then passed to the merge
    * operator.
    *
    * @param entriesSameDomain to merge, at least one
    * @param op merge operator from HList to the result type or None
    * @return a TSEntry of the result type or None
    */
  private def mergeSameDomain[T <: HList, ZO <: HList, R](entriesSameDomain: Seq[TSEntry[T]])(op: T => Option[R])(
      // implicits for mergeHLists
      implicit zipper: Zip.Aux[T :: T :: HNil, ZO],
      mapper: Mapper.Aux[Merge.type, ZO, T]
  ): Option[TSEntry[R]] =
    entriesSameDomain match {
      // check that all entries have the same start timestamp and end
      case TSEntry(ts, _, d) +: others if others.forall(e => e.timestamp == ts && e.validity == d) =>
        op(entriesSameDomain.map(_.value).reduce(mergeHLists[T, ZO])).map(TSEntry(ts, _, d))

      case _ =>
        throw new IllegalArgumentException("Can only merge a non-empty sequence of entries with exactly the same domain.")
    }

  // Shapeless merge operator for HLists of Options.
  private def mergeHLists[T <: HList, ZO <: HList](accumulator: T, element: T)(
      implicit zipper: Zip.Aux[T :: T :: HNil, ZO],
      mapper: Mapper.Aux[Merge.type, ZO, T]
  ): T = {
    (accumulator zip element) map Merge
  }

  // Shapeless type class for the merge operator. It compares the two options at
  // the same position in the two HLists. If the first one is defined it takes that
  // value. Otherwise it takes the second option.
  private object Merge extends Poly1 {
    implicit def default[A]: Case.Aux[(Option[A], Option[A]), Option[A]] =
      at { case (accOpt, elemOpt) => accOpt.orElse(elemOpt) }
  }

  /** Create a TSEntry with the given value or None. */
  private def applyEmptyMerge[R](from: Long, to: Long)(allNone: Option[R]): Option[TSEntry[R]] =
    if (from >= to) None
    else allNone.map(TSEntry(from, _, to - from))

  /** Create an HList of only None with the given shapeless length. */
  private def allNoneHList(length: Nat)(implicit fill: Fill[length.N, None.type]): fill.Out =
    HList.fill(length)(None)(fill)

  /** Return the shapeless length of an HList type. */
  private def hListLength[T <: HList](implicit length: Length[T]): length.Out = length()
}
// scalastyle:on non.ascii.character.disallowed
