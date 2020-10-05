package io.sqooba.oss.timeseries.entity

import io.sqooba.oss.timeseries.labels.TsLabel

import scala.util.Try

trait TimeSeriesEntityId {

  /**
    * @return the key prefix for the implementing identifier.
    */
  def keyPrefix: TsKeyPrefix

  /**
    * @return the internal identification number of whatever the implementor identifies.
    */
  def id: Long

  /**
    * @param signal the name of the sensor we want to build the TSDB identifier for
    * @return the full key identifying a time series in the TSDB.
    */
  // TODO: the use of this function probably causes AnyVal implementations to require an instantiation.
  // At the moment we mostly care about correctness, not performance, but we need to keep it in mind.
  // If at some point we want to do things allocation-free, have a look at
  // https://docs.scala-lang.org/overviews/core/value-classes.html
  // under the extension methods paragraph
  def buildTimeseriesID(signal: TsLabel): TsId

  /**
    *  Gets the entity Id, formatted as PREFIX_ID
    */
  def formatted: String

}

abstract class GenericEntityId(override val id: Long, override val keyPrefix: TsKeyPrefix) extends TimeSeriesEntityId {
  override def buildTimeseriesID(signal: TsLabel): TsId = GenericTsId(this, signal)
  override val formatted: String                        = s"${keyPrefix.prefix}${GenericTsId.prefixSeparator}$id"
}

case class TripEntityId(id: Long, keyPrefix: TsKeyPrefix) extends TimeSeriesEntityId {
  override def buildTimeseriesID(signal: TsLabel): TsId = TripTsId(this, signal)
  override val formatted: String                        = s"${keyPrefix.prefix}${TripTsId.prefixSeparator}$id"
}

object TimeSeriesEntityId {

  /** Builds a [[TimeSeriesEntityId]] from a given prefix and id. */
  def from(prefix: String, id: Long)(implicit entityParser: EntityParser): Try[TimeSeriesEntityId] =
    entityParser.parseKeyPrefix(prefix).map(_.toEntityId(id))

}
