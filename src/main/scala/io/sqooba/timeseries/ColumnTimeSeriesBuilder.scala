package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.{ColumnTimeSeries, EmptyTimeSeries, TSEntry}

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable

/**
  * A builder intended to ease the assembling of entries into a ColumnTimeSeries.
  * Uses a vector builder and a small stack under the hood.
  */
class ColumnTimeSeriesBuilder[T](compress: Boolean = true) extends TimeSeriesBuilderTrait[T] {

  // TODO unify/refactor the Builder implementations to reduce duplication

  // Contains finalized entries (ie, they won't be trimmed or extended anymore)
  private val resultBuilder = (new VectorBuilder[Long], new VectorBuilder[T], new VectorBuilder[Long])

  // Contains the last added entry: we need to keep it around
  // as it may be subject to trimming or extension
  private var lastAdded: Option[TSEntry[T]] = None

  private var resultCalled = false

  private var isDomainContinuous = true

  override def +=(elem: TSEntry[T]): ColumnTimeSeriesBuilder.this.type = {
    lastAdded = lastAdded match {
      // Nothing added yet
      case None => Some(elem)

      // A previous entry exists: attempt to append the new one
      case Some(last) =>
        // Ensure that we don't throw away older entries
        if (elem.timestamp <= last.timestamp) {
          throw new IllegalArgumentException(
            s"Elements should be added chronologically (here last timestamp was ${last.timestamp} and the one added was ${elem.timestamp}"
          )
        }

        // set continuous flag to false if there is a hole between the two entries
        isDomainContinuous = last.definedUntil >= elem.timestamp

        last.appendEntry(elem, compress) match {
          // If compression occurred. Keep that entry around
          case Seq(compressed) => Some(compressed)

          // If no compression:
          // - add the first element to the builders, as they won't change anymore
          // - keep the second around, as it may be subject to trimming or extending in the future
          case Seq(first, second) =>
            addToBuilder(first)
            Some(second)
        }
    }
    this
  }

  override def clear(): Unit = {
    resultBuilder._1.clear()
    resultBuilder._2.clear()
    resultBuilder._3.clear()
    lastAdded = None
    resultCalled = false
  }

  override def result(): TimeSeries[T] =
    ColumnTimeSeries.ofColumnVectorsUnsafe(vectorResult(), compress, isDomainContinuous)

  def vectorResult(): (Vector[Long], Vector[T], Vector[Long]) = {
    if (resultCalled) {
      throw new IllegalStateException("result can only be called once, unless the builder was cleared.")
    }
    lastAdded.foreach(addToBuilder)
    resultCalled = true

    (resultBuilder._1.result, resultBuilder._2.result, resultBuilder._3.result)
  }

  private def addToBuilder(entry: TSEntry[T]): Unit = {
    resultBuilder._1 += entry.timestamp
    resultBuilder._2 += entry.value
    resultBuilder._3 += entry.validity
  }

  /**
    * @return the end of the domain of validity of the last entry added to this builder
    */
  def definedUntil: Option[Long] =
    lastAdded.map(_.definedUntil)

}
