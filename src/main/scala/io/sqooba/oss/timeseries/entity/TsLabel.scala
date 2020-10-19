package io.sqooba.oss.timeseries.entity

/** A time series label. It identifies a type/kind of a time series signal, like "power
  * output", "temperature".
  *
  * Units can be inferred by specifying the (implicit) LabelUnitMapper
  */
case class TsLabel(value: String) {

  def unit(implicit unitMapper: LabelUnitMapper): Option[String] = unitMapper.deriveUnit(this)

}
