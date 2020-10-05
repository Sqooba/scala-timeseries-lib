package io.sqooba.oss.timeseries.entity

/**
  * Represents a key prefix to distinguish between different types of entities for which
  * data is stored
  */
trait TsKeyPrefix {

  val prefix: String

  override def toString: String = prefix

  def toEntityId(id: Long): TimeSeriesEntityId
}

case class GenericKeyPrefix(prefix: String) extends TsKeyPrefix {

  def toEntityId(id: Long): TimeSeriesEntityId = new GenericEntityId(id, this) {}
}
