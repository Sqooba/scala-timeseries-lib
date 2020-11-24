package io.sqooba.oss.timeseries.entity

/** Identifies a specific time series type (with the label) for a specific time series
 * entity.
 *
 * @param entityId specifies the entity
 * @param label specifies the signal
 * @tparam I serves to specify a certain implementation of entity id
 */
final case class TsId[I <: TimeSeriesEntityId](entityId: I, label: TsLabel)
