package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.TSEntry

/**
  * This encapsulates the logic of fitting TSEntries one after another. It is used in TimeSeriesBuilders to take
  * care of trimming, compressing and checking the sanity of consecutive entries.
  *
  * @param compress Whether consecutive entries of equal value should be compressed into one or not.
  */
class TSEntryFitter[T] private[timeseries] (compress: Boolean) {

  // Contains the last added entry: we need to keep it around
  // as it may be subject to trimming or extension
  private var lastAdded: Option[TSEntry[T]] = None

  private var isDomainCont = true

  /**
    * Adds the next timeseries entry. The previously added entry will be trimmed if it overlaps with the next entry.
    * If the next entry has the same value as the previous one, they are compressed into one entry and nothing
    *  (None) is returend. Otherwise, the previous entry is trimmed and returned (Some(prev)).
    *
    * @param elem the next TSEntry of the series
    * @return the trimmed and compressed previous entry or None
    */
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

  /**
    * @return the last entry that was added to the fitter. This entry can still change if more entries are added
    *         (it might be compressed/trimmed).
    */
  def lastEntry: Option[TSEntry[T]] = lastAdded

  /**
    * @return whether all added entries so far were either contiguous or overlapping. I.e. there were no holes
    *         in the domain of definition of the entries seen so far.
    */
  def isDomainContinuous: Boolean = isDomainCont

  /**
    * Clears/resets all state of the fitter.
    */
  def clear(): Unit = {
    lastAdded = None
    isDomainCont = true
  }
}
