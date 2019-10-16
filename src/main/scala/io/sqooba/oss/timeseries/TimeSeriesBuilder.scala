package io.sqooba.oss.timeseries

import io.sqooba.oss.timeseries.immutable.TSEntry

/**
  * Unifies the interface for builders of timeseries implementations. This enables generic test cases.
  */
// TODO: can we make this trait more useful and hence reduce the boilerplate needed in each individual builder?
trait TimeSeriesBuilder[T] {

  /** Adds a single element to the builder.
    *  @param elem the element to be added.
    *  @return the builder itself.
    */
  def +=(elem: TSEntry[T]): this.type = addOne(elem)

  def addOne(elem: TSEntry[T]): this.type

  def ++=(xs: Seq[TSEntry[T]]): this.type = {
    xs foreach +=
    this
  }

  /** Produces a collection from the added elements.  This is a terminal operation:
    *  the builder's contents are undefined after this operation, and no further
    *  methods should be called.
    *
    *  @return a collection containing the elements added to this builder.
    */
  def result(): TimeSeries[T]

  /** Clears the contents of this builder.
    *  After execution of this method the builder will contain no elements.
    */
  def clear(): Unit

  /**
    * @return the end of the domain of validity of the last entry added to this builder. None if nothing added yet.
    */
  def definedUntil: Option[Long]
}
