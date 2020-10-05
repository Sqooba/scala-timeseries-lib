package io.sqooba.oss.timeseries.labels

/** A time series label.
  *
  * Units can be inferred by specifying the (implicit) LabelUnitMapper
  */
case class TsLabel(value: String) {

  def unit(implicit unitMapper: LabelUnitMapper): Option[String] = unitMapper.deriveUnit(this)

  // A label should never contain a key separator, since it would break the formatting
  // require(!value.contains(TsId.keySeparator))
}
