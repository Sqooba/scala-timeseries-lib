package io.sqooba.oss.timeseries.entity

import io.sqooba.oss.timeseries.labels.{LabelUnitMapper, TsLabel}

import scala.annotation.implicitNotFound
import scala.util.{Failure, Success, Try}

/** Encapsulates the parsing logic for [[TsLabel]], [[TsId]] and
  * [[TimeSeriesEntityId]]. It lets the user provide their own mapping from string
  * keys to entities and to units.
  */
@implicitNotFound("Cannot parse entities and/or units for TsId/TsLabel because no parser was defined.")
trait EntityParser extends LabelUnitMapper {

  /** Parse the unit of a TsLabel from the given string key.
    *
    * @return an option containing the string representation of the unit
    */
  def deriveUnit(in: TsLabel): Option[String]

  /** Parse the key prefix of a TsId from the given string key.
    *
    * @return a TsKeyPrefix or a Failure
    */
  def parseKeyPrefix(string: String): Try[TsKeyPrefix]
}

object EntityParser {

  /** Default parser that returns [[GenericKeyPrefix]]es and no units. */
  val default: EntityParser = new EntityParser {

    override def deriveUnit(in: TsLabel): Option[String] = None

    override def parseKeyPrefix(string: String): Try[TsKeyPrefix] = Success(GenericKeyPrefix(string))

  }

  val hexEntityParser: EntityParser = default

}
