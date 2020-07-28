package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.archive.GorillaBlock

import scala.reflect.runtime.universe

/**
  * A timeseries based on TSEntries containing inner timeseries. The outer series
  * needs to have at least two entries and the inner timeseries cannot by empty.
  * Also, the looseDomain of each TSEntry should correspond to the looseDomain of the
  * series it contains.
  *
  */
case class NestedTimeSeries[+T] private (
    underlying: TimeSeries[TimeSeries[T]]
) extends TimeSeries[T] {

  require(
    underlying.size >= 2,
    "A NestedTimeSeries can not be empty (should be an EmptyTimeSeries) nor contain only one inner series (should simply be the inner series)"
  )

  // TODO move to somewhere where we don't need to iterate over all nested series at
  //   each instantiation
  require(
    underlying.entries.forall(_.value.nonEmpty),
    "A NestedTimeSeries cannot contain entries of inner series that are empty."
  )

  def at(t: Long): Option[T] = underlying.at(t).flatMap(_.at(t))

  def entryAt(t: Long): Option[TSEntry[T]] = underlying.at(t).flatMap(_.entryAt(t))

  // As this is used for queries to the head and last, we don't want to
  // construct a large sequence of all entries each time, therefore we use a stream.
  def entries: Seq[TSEntry[T]] = underlying.entries.toStream.flatMap(_.value.entries)

  override def values: Seq[T] = underlying.values.flatMap(_.values)

  def headOption: Option[TSEntry[T]] = underlying.head.value.headOption
  def lastOption: Option[TSEntry[T]] = underlying.last.value.lastOption

  def looseDomain: TimeDomain = ContiguousTimeDomain(head.timestamp, last.definedUntil)

  lazy val supportRatio: Double =
    underlying.entries.map(e => e.value.looseDomain.size * e.value.supportRatio).sum / looseDomain.size

  lazy val size: Int = underlying.entries.map(_.value.size).sum

  val isEmpty: Boolean = false

  lazy val isCompressed: Boolean = underlying.entries.forall(_.value.isCompressed)

  lazy val isDomainContinuous: Boolean = underlying.entries.forall(_.value.isDomainContinuous)

  /**
    * Maps the underlying timeseries, then chooses an appropriate result
    * implementation.
    */
  private def mapInnerSeries[O: universe.WeakTypeTag](f: TimeSeries[T] => TimeSeries[O]): TimeSeries[O] =
    NestedTimeSeries.chooseUnderlying(
      underlying.map(f).filterEntries(_.value.nonEmpty)
    )

  def mapEntries[O: universe.WeakTypeTag](f: TSEntry[T] => O, compress: Boolean): TimeSeries[O] =
    mapInnerSeries(_.mapEntries(f, compress))

  def filterEntries(predicate: TSEntry[T] => Boolean): TimeSeries[T] =
    mapInnerSeries(_.filterEntries(predicate))

  override def fill[U >: T](whenUndef: U): TimeSeries[U] =
    mapInnerSeries(_.fill(whenUndef))

  def trimRight(at: Long): TimeSeries[T] =
    mapInnerSeries(_.trimRight(at)).trimRight(at)

  def trimRightDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    mapInnerSeries(_.trimRightDiscrete(at, includeEntry)).trimRightDiscrete(at)

  def trimLeft(at: Long): TimeSeries[T] =
    mapInnerSeries(_.trimLeft(at)).trimLeft(at)

  def trimLeftDiscrete(at: Long, includeEntry: Boolean): TimeSeries[T] =
    mapInnerSeries(_.trimLeftDiscrete(at, includeEntry)).trimLeftDiscrete(at, includeEntry)

}

object NestedTimeSeries {

  /**
    * Constructs a TimeSeries from buckets (TSEntries containg a TimeSeries).
    * If there are at least two buckets, this builds a nested TimeSeries,
    * otherwise the contained TimeSeries in the single bucket is directly used.
    *
    * @param buckets TSEntries containing inner TimeSeries as values
    *                The TSEntries need to be a well-formed sequence of entries and
    *                their timestamp and validity have to correspond to the
    *                looseDomain of the contained TimeSeries.
    * @return a possibly nested TimeSeries
    */
  def ofOrderedEntriesSafe[T](
      buckets: Seq[TSEntry[TimeSeries[T]]]
  ): TimeSeries[T] =
    chooseUnderlying(TimeSeries.ofOrderedEntriesSafe(buckets))

  /** Constructs a TimeSeries from a sequence of GorillaBlocks in TSEntries. These will
    * typically come from a GorillaSuperBlock (file).
    *
    * @param blocks The TSEntries containing the blocks need to be a well-formed
    *                sequence of entries and their timestamp and validity have to
    *                correspond to the looseDomain of the contained TimeSeries.
    * @return a possibly nested TimeSeries
    */
  def ofGorillaBlocks(blocks: Seq[TSEntry[GorillaBlock]]): TimeSeries[Double] =
    ofOrderedEntriesSafe(
      blocks.map(_.map(GorillaBlockTimeSeries(_)))
    )

  // Chooses the implementation for a given nested timeseries. If there is only one
  // inner timeseries in the nested series, we directly return this. Otherwise the
  // series is wrapped by a NestedTimeSeries.
  private[NestedTimeSeries] def chooseUnderlying[O](nested: TimeSeries[TimeSeries[O]]): TimeSeries[O] =
    nested.size match {
      case 0 => EmptyTimeSeries
      case 1 => nested.headValue
      case _ => new NestedTimeSeries[O](nested)
    }
}
