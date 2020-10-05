package io.sqooba.oss.timeseries.entity

import io.sqooba.oss.timeseries.labels.TsLabel

import scala.util.Try

trait TsId {
  val entityId: TimeSeriesEntityId
  val label: TsLabel
  val id: String
}

/** Identifies a single materialized time series.
  *
  * @param entityId An entity which has time series data
  * @param label    The label of the time series (e.g sensor data)
  */
case class TripTsId(entityId: TimeSeriesEntityId, label: TsLabel) extends TsId {
  import TripTsId._

  /** Formats the [[TsId]] into a string of the format: LABEL-PREFIX_ID */
  override val id: String = s"${label.value}$keySeparator${entityId.formatted}"
}

object TripTsId {

  val keySeparator    = "-"
  val prefixSeparator = "_"

  /** Tries to construct a [[TsId]] from a raw string representation. By using the
    * given entityParser it will try to parse the entity type and unit of the label.
    *
    * The label is a string (anything is accepted).
    * The idRaw is in the format PREFIX_ID.
    *   - PREFIX must be a known prefix, by the given entityParser
    *   - ID must be a number
    */
  def from(label: String, idRaw: String)(implicit entityParser: EntityParser): Try[TsId] = {
    val idTokens = idRaw.split(prefixSeparator)

    for {
      idLong <- Try(idTokens(1).toLong)
      id     <- TimeSeriesEntityId.from(idTokens(0), idLong)
    } yield TripTsId(id, TsLabel(label))
  }

}

/** Identifies a single materialized time series.
  *
  * @param entityId An entity which has time series data
  * @param label    The label of the time series (e.g sensor data)
  */
case class GenericTsId(entityId: TimeSeriesEntityId, label: TsLabel) extends TsId {
  import GenericTsId._

  /** Formats the [[TsId]] into a string of the format: LABEL-PREFIX_ID */
  override val id: String = s"${label.value}$keySeparator${entityId.formatted}"
}

object GenericTsId {

  val keySeparator    = "-"
  val prefixSeparator = "_"

  /** Tries to construct a [[TsId]] from a raw string representation. By using the
    * given entityParser it will try to parse the entity type and unit of the label.
    *
    * The label is a string (anything is accepted).
    * The idRaw is in the format PREFIX_ID.
    *   - PREFIX must be a known prefix, by the given entityParser
    *   - ID must be a number
    */
  def from(label: String, idRaw: String)(implicit entityParser: EntityParser): Try[TsId] = {
    val idTokens = idRaw.split(prefixSeparator)

    for {
      idLong <- Try(idTokens(1).toLong)
      id     <- TimeSeriesEntityId.from(idTokens(0), idLong)
    } yield GenericTsId(id, TsLabel(label))
  }
}

