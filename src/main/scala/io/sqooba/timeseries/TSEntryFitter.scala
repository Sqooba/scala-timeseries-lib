package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.TSEntry

class TSEntryFitter[T] private[timeseries] (compress: Boolean) {

  // Contains the last added entry: we need to keep it around
  // as it may be subject to trimming or extension
  private var lastAdded: Option[TSEntry[T]] = None

  private var isDomainCont = true

  def addAndFitLast(elem: TSEntry[T]): Option[TSEntry[T]] = {
    val (newLast, fitted) = lastAdded match {
      // First Entry: save it and return no fitted entry
      case None => (Some(elem), None)

      // A previous entry exists: attempt to append the new one
      case Some(last) =>
        // Ensure that we don't throw away older entries
        if (elem.timestamp <= last.timestamp) {
          throw new IllegalArgumentException(
            s"Elements should be added chronologically (here last timestamp was ${last.timestamp} and the one added was ${elem.timestamp}"
          )
        }

        // set continuous flag to false if there is a hole between the two entries
        isDomainCont = last.definedUntil >= elem.timestamp

        last.appendEntry(elem, compress) match {
          // A compression occurred. Keep that entry around, return no fitted
          case Seq(compressed) => (Some(compressed), None)

          // No further compression:
          // - return the finished entry
          // - keep the second around
          case Seq(finished, second) => (Some(second), Some(finished))
        }
    }

    lastAdded = newLast
    fitted
  }

  def lastEntry: Option[TSEntry[T]] = lastAdded

  def isDomainContinuous: Boolean = isDomainCont

  def clear(): Unit = {
    lastAdded = None
    isDomainCont = true
  }
}
