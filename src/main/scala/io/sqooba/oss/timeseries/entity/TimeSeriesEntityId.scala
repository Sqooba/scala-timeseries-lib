package io.sqooba.oss.timeseries.entity

trait TimeSeriesEntityId {

  /**
    * @return the internal identification number of whatever the implementor identifies.
    */
  def id: Long

  /**
    * @param signal the label of the time series signal
    * @return a TsId that fully identifies a time series signal of a the entity
    *         specified by this EntityId
    */
  // TODO: the use of this function probably causes AnyVal implementations to require an
  //   instantiation. At the moment we mostly care about correctness, not performance, but
  //   we need to keep it in mind. If at some point we want to do things allocation-free,
  //   have a look at https://docs.scala-lang.org/overviews/core/value-classes.html under
  //   the extension methods paragraph
  def buildTsId(signal: TsLabel): TsId = TsId(this, signal)
}
