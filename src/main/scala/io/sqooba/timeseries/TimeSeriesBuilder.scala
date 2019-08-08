package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.TSEntry

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable

/**
  * A builder intended to ease the assembling of entries into a time series.
  * Uses a vector builder and a small stack under the hood.
  */
class TimeSeriesBuilder[T](compress: Boolean = true) extends TimeSeriesBuilderTrait[T] {

  // Contains finalized entries (ie, they won't be trimmed or extended anymore)
  private val resultBuilder = new VectorBuilder[TSEntry[T]]

  // Contains the last added entry: we need to keep it around
  // as it may be subject to trimming or extension
  private var lastAdded: Option[TSEntry[T]] = None

  private var resultCalled = false

  private var isDomainContinuous = true

  override def +=(elem: TSEntry[T]): TimeSeriesBuilder.this.type = {
    lastAdded = lastAdded match {
      // First Entry!
      case None =>
        Some(elem)
      // A previous entry exists: attempt to append the new one
      case Some(last) =>
        // Ensure that we don't throw away older entries
        if (elem.timestamp <= last.timestamp) {
          val errorMess = s"Elements should be added chronologically (here last timestamp was ${last.timestamp} and the one added was ${elem.timestamp}"
          throw new IllegalArgumentException(errorMess)
        }

        // set continuous flag to false if there is a hole between the two entries
        isDomainContinuous = last.definedUntil >= elem.timestamp

        last.appendEntry(elem, compress) match {
          // A compression occurred. Keep that entry around
          case Seq(compressed) =>
            Some(compressed)
          // No compression:
          // - add the first element to the builder, as it won't change anymore
          // - keep the second around, as it may be subject to trimming or extending in the future
          case Seq(first, second) =>
            resultBuilder += first
            Some(second)
        }
    }
    this
  }

  override def clear(): Unit = {
    resultBuilder.clear()
    lastAdded = None
    resultCalled = false
  }

  override def result(): TimeSeries[T] =
    TimeSeries.ofOrderedEntriesUnsafe(vectorResult(), isCompressed = compress, isDomainContinuous)

  def vectorResult(): Vector[TSEntry[T]] = {
    if (resultCalled) {
      throw new IllegalStateException("result can only be called once, unless the builder was cleared.")
    }
    lastAdded.foreach(resultBuilder += _)
    resultCalled = true
    resultBuilder.result()
  }

  def definedUntil: Option[Long] = lastAdded.map(_.definedUntil)

}
