package io.sqooba.oss.timeseries.entity

/** Identifies a specific time series type (with the label) for a specific time series
  * entity.
  *
  * @param entityId specifies the entity
  * @param label specifies the signal
  */
final case class TsId(entityId: TimeSeriesEntityId, label: TsLabel)
