package io.sqooba.oss.timeseries.zio

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.validation.TimestampValidator
import zio.{Ref, Task, UIO, ZIO}

/**
  * This encapsulates the logic of fitting TSEntries one after another. It is used to take
  * care of trimming, compressing and checking the sanity of consecutive entries.
  *
  * @param compress Whether consecutive entries of equal value should be compressed into one or not.
  */
class ZEntryFitter[T](
                       compress: Boolean,
                       // Contains the last added entry: we need to keep it around
                       // as it may be subject to trimming or extension
                       lastAddedRef: Ref[Option[TSEntry[T]]],
                       isDomainContRef: Ref[Boolean]
                     ) {

  def addAndFitLast(addMe: TSEntry[T]): Task[Option[TSEntry[T]]] = {
    for {
      previouslyAdded <- lastAddedRef.get
      previouslyContinuous <- isDomainContRef.get
      (newlyAdded, fitted, continuous) <- trimOrExpand(previouslyAdded, addMe)
      _ <- lastAddedRef.set(newlyAdded) *>
        isDomainContRef.set(previouslyContinuous && continuous)
    } yield fitted
  }

  private def trimOrExpand(last: Option[TSEntry[T]], added: TSEntry[T])
  // Return a triple of: 'last added entry', 'fitted entry', 'domain continuous
  // (Or a failed task if the added entry is invalid)
  : Task[(Option[TSEntry[T]], Option[TSEntry[T]], Boolean)] = {
    last match {
      // First Entry: save it and return no fitted entry
      case None => ZIO.succeed((Some(added), None, true))

      // A previous entry exists: attempt to append the new one
      case Some(last) =>
        TimestampValidator.validate(last.timestamp, added.timestamp)

        for {
          _ <- validateTimestamps(last.timestamp, added.timestamp)
          isDomainCont = last.definedUntil >= added.timestamp
        } yield last.appendEntry(added, compress) match {
          // A compression occurred. Keep that entry around, return no fitted
          case Seq(compressed) => (Some(compressed), None, isDomainCont)

          // No further compression:
          // - return the finished entry
          // - keep the second around
          case Seq(finished, second) => (Some(second), Some(finished), isDomainCont)
        }
    }
  }

  private def validateTimestamps(lastTs: Long, currentTs: Long) = {
    if (lastTs < currentTs) {
      ZIO.succeed(())
    } else {
      ZIO.fail(new IllegalArgumentException(s"The timestamps need to be strictly increasing, was $lastTs before $currentTs."))
    }
  }

  /** @return the last entry that was added to the fitter. This entry can still change
    *         if more entries are added (it might be compressed/trimmed).
    */
  def lastEntry: UIO[Option[TSEntry[T]]] = lastAddedRef.get

  /** @return whether all added entries so far were either contiguous or overlapping.
    *         I.e. there were no holes in the domain of definition of the entries seen so far.
    */
  def isDomainContinuous: UIO[Boolean] = isDomainContRef.get

}

object ZEntryFitter {

  def init[T](compress: Boolean): UIO[ZEntryFitter[T]] =
    for {
      initPrevious <- Ref.make[Option[TSEntry[T]]](None)
      continuous <- Ref.make(true)
    } yield new ZEntryFitter[T](compress, initPrevious, continuous)

}

