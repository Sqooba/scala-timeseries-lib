package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.labels.TsLabel

import scala.util.{Success, Try}

package object entity {

  // Some fruits to test
  case class BananaId(override val id: Long) extends GenericEntityId(id, BananaPrefix)

  case class AppleId(override val id: Long) extends GenericEntityId(id, ApplePrefix)

  case class CherryId(override val id: Long) extends GenericEntityId(id, CherryPrefix)

  case object BananaPrefix extends TsKeyPrefix {
    val prefix: String = "ba"

    def toEntityId(id: Long): TimeSeriesEntityId = BananaId(id)
  }

  case object ApplePrefix extends TsKeyPrefix {
    val prefix: String = "ap"

    def toEntityId(id: Long): TimeSeriesEntityId = AppleId(id)
  }

  case object CherryPrefix extends TsKeyPrefix {
    val prefix: String = "ch"

    def toEntityId(id: Long): TimeSeriesEntityId = CherryId(id)
  }

  val fruitParser: EntityParser = new EntityParser {

    def parseUnit(string: String): Try[Option[String]] = Success(None)

    def parseKeyPrefix(string: String): Try[TsKeyPrefix] =
      Try(
        string match {
          case BananaPrefix.prefix => BananaPrefix
          case ApplePrefix.prefix  => ApplePrefix
          case CherryPrefix.prefix => CherryPrefix
        }
      )

    /** Parse the unit of a TsLabel from the given string key.
      *
      * @return an option containing the string representation of the unit
      */
    override def deriveUnit(in: TsLabel): Option[String] = None /* in.value match {
      case BananaPrefix.prefix => BananaPrefix
      case ApplePrefix.prefix  => ApplePrefix
      case CherryPrefix.prefix => CherryPrefix
    } */
  }
}
