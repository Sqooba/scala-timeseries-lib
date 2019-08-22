package io.sqooba.oss.timeseries.immutable

import io.sqooba.oss.timeseries.archive.GorillaBlock
import io.sqooba.oss.timeseries.{TSEntryFitter, TimeSeries, TimeSeriesBuilder}

import scala.reflect.runtime.universe._

/**
  * TimeSeries implementation based on one block of a Gorilla compressed timeseries.
  * The block should not be too long, as lookups are done by scan and thus take
  * linear time.
  */
case class GorillaBlockTimeSeries private (
    block: GorillaBlock,
    size: Int,
    isCompressed: Boolean = false,
    isDomainContinuous: Boolean = false
) extends TimeSeries[Double] {

  def entries: Seq[TSEntry[Double]] = GorillaBlock.decompress(block)

  def head: TSEntry[Double] = entries.head

  def headOption: Option[TSEntry[Double]] = entries.headOption

  def last: TSEntry[Double] = entries.last

  def lastOption: Option[TSEntry[Double]] = entries.lastOption

  def at(t: Long): Option[Double] = entryAt(t).map(_.value)

  def entryAt(t: Long): Option[TSEntry[Double]] =
    entries.takeWhile(_.timestamp <= t).find(_.defined(t))

  def isEmpty: Boolean = false

  def looseDomain: TimeDomain = head.looseDomain.looseUnion(last.looseDomain)

  // TODO: if one could only decode the validities, this would suffice here
  //  tracked by T546
  lazy val supportRatio: Double =
    entries.map(_.looseDomain.size).sum.toFloat / looseDomain.size

  def map[O: WeakTypeTag](f: Double => O, compress: Boolean = true): TimeSeries[O] =
    mapEntries[O](e => f(e.value), compress)

  def mapWithTime[O: WeakTypeTag](f: (Long, Double) => O, compress: Boolean = true): TimeSeries[O] =
    mapEntries[O](e => f(e.timestamp, e.value), compress)

  private def mapEntries[O: WeakTypeTag](f: TSEntry[Double] => O, compress: Boolean = true): TimeSeries[O] =
    entries
      .map(e => TSEntry(e.timestamp, f(e), e.validity))
      .foldLeft(newBuilder[O](compress))(_ += _)
      .result()

  def filter(predicate: TSEntry[Double] => Boolean): TimeSeries[Double] =
    GorillaBlockTimeSeries.ofOrderedEntriesSafe(
      entries.filter(predicate).toStream
    )

  def filterValues(predicate: Double => Boolean): TimeSeries[Double] =
    filter(e => predicate(e.value))

  def fill[U >: Double](whenUndef: U): TimeSeries[U] =
    TimeSeries
      .fillGaps(entries, whenUndef)
      .foldLeft(newBuilder[U]())(_ += _)
      .result()

  def trimRight(t: Long): TimeSeries[Double] =
    // trimRight can handle the case where t is before the timestamp of the entry
    // therefore we use it rather than trimEntryRight
    trimRightWithFunction(e => e.trimRight(t), t)

  def trimRightDiscrete(t: Long, includeEntry: Boolean): TimeSeries[Double] =
    trimRightWithFunction(e => e.trimRightDiscrete(t, includeEntry), t)

  private def trimRightWithFunction(
      trim: TSEntry[Double] => TimeSeries[Double],
      t: Long
  ): TimeSeries[Double] = {
    // find entry on cut
    val trimmed = entries.span(_.definedUntil < t) match {
      case (leftEntries, Stream()) => leftEntries

      // an entry lies on the cut
      case (leftEntries, entry #:: _) =>
        leftEntries ++ trim(entry).entries
    }

    GorillaBlockTimeSeries.ofOrderedEntriesSafe(trimmed)
  }

  def trimLeft(t: Long): TimeSeries[Double] =
    trimLeftWithFunction(e => e.trimLeft(t), t)

  def trimLeftDiscrete(t: Long, includeEntry: Boolean): TimeSeries[Double] =
    trimLeftWithFunction(e => e.trimLeftDiscrete(t, includeEntry), t)

  private def trimLeftWithFunction(
      trim: TSEntry[Double] => TimeSeries[Double],
      t: Long
  ): TimeSeries[Double] = {
    // find entry on cut
    val trimmed = entries.span(_.definedUntil < t) match {
      // all entries are before the cut
      case (_, Stream()) => Stream()

      // an entry lies on the cut
      case (_, entry #:: rightEntries) =>
        trim(entry).entries.toStream ++ rightEntries
    }

    GorillaBlockTimeSeries.ofOrderedEntriesSafe(trimmed)
  }

  override def newBuilder[U](compress: Boolean)(implicit tag: WeakTypeTag[U]): TimeSeriesBuilder[U] =
    // Switch the used builder and therefore the resulting implementation of the
    // timeseries depending on the type U.
    // This enables us to just use the builder everywhere in the implementations.
    // It takes care of whether the result needs to be encoded again or not.
    tag match {
      // create a new gorilla encoded timeseries for doubles
      case t if t == weakTypeTag[Double] =>
        GorillaBlockTimeSeries.newBuilder(compress).asInstanceOf[TimeSeriesBuilder[U]]

      // use the default implementation for all other types
      case _ =>
        TimeSeries.newBuilder(compress)
    }
}

object GorillaBlockTimeSeries {

  /**
    * Construct a Gorilla encoded timeseries using its own builder given an ordered
    * list of entries. The correct underlying implementation will be chosen
    * (EmptyTimeSeries, TSEntry or PackedTimeSeries).
    *
    * @param entries A sequence of TSEntries which HAS to be chronologically ordered
    *                (w.r.t. their timestamps) and  well-formed (no duplicated timestamps)
    * @param compress A flag specifying whether consecutive entries should be compressed or not.
    * @return a packed, Gorilla encoded timeseries
    */
  def ofOrderedEntriesSafe(
      entries: Seq[TSEntry[Double]],
      compress: Boolean = true
  ): TimeSeries[Double] =
    entries.foldLeft(newBuilder(compress))(_ += _).result()

  /**
    * @return the builder for the Gorilla compressed timeseries implementation
    */
  def newBuilder(compress: Boolean = true): TimeSeriesBuilder[Double] = new Builder(compress)

  private class Builder(compress: Boolean = true) extends TimeSeriesBuilder[Double] {

    // Contains finalized entries
    private val blockBuilder = new GorillaBlock.Builder(compress)

    private var currentSize  = 0
    private var resultCalled = false

    override def +=(elem: TSEntry[Double]): this.type = {
      currentSize += 1
      blockBuilder += elem
      this
    }

    override def clear(): Unit = {
      blockBuilder.clear()
      currentSize = 0
      resultCalled = false
    }

    override def result(): TimeSeries[Double] = {
      if (resultCalled) {
        throw new IllegalStateException("Cannot call result more than once, unless the builder was cleared.")
      }

      resultCalled = true

      currentSize match {
        case 0 => EmptyTimeSeries
        case 1 => blockBuilder.lastEntry.get
        case _ =>
          new GorillaBlockTimeSeries(
            blockBuilder.result(),
            currentSize,
            compress,
            blockBuilder.isDomainContinuous
          )
      }
    }

    def definedUntil: Option[Long] = blockBuilder.lastEntry.map(_.definedUntil)
  }
}
