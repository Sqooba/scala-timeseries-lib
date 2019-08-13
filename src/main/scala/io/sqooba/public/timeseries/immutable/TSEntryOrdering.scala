package io.sqooba.public.timeseries.immutable

/**
  * Defines an ordering for TSEntries. Should reduce the amount of boxing:
  * seq.sortBy(_.timestamp) induces _a lot_ of boxing to Long.
  *
  * Not having the _.timestamp lambda returning a Long should improve things.
  */
object TSEntryOrdering extends Ordering[TSEntry[Any]] {

  override def compare(x: TSEntry[Any], y: TSEntry[Any]): Int =
    x.timestamp compare y.timestamp

}
