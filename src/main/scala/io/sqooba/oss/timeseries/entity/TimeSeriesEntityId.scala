package io.sqooba.oss.timeseries.entity

/** Identifies an entity that can have time series data attached to it. The different
 * possible signals are defined by [[io.sqooba.oss.timeseries.entity.TsLabel!]]s.
 *
 * @note It is the implementor's responsibility to choose a way of identifying
 * entities. Be that by different type, numerical or lexical identifiers. The idea is
 * that users define the identifying attributes on their implementation of the trait.
 */
// Not requiring a specific type and shape of identifying property leaves the choice
// to the user and also enables use-cases where multiple identifying mechanisms need
// to be combined with trait polymorphism.
trait TimeSeriesEntityId {

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
  def buildTsId(signal: TsLabel): TsId[this.type] = TsId(this, signal)
}
