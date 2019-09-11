package io.sqooba.oss.timeseries.archive

import io.sqooba.oss.timeseries.{TimeSeries, TimeSeriesBuilder}
import io.sqooba.oss.timeseries.immutable.TSEntry

/**
  * Helper for bucketing/slicing a stream of TSEntries in order to be stored as
  * a packed timeseries in Gorilla TSC encoded format.
  */
object TimeBucketer {

  /**
    * Buckets the passed entries into blocks/chunks that have domains delimited by the given
    * bucketTimstamps. If such a block exceeds the passed maximal number of entries in size
    * then it is further split up into multiple blocks within the bucket timestamp domain.
    *
    * A block is represented by a TSEntry containing the entries of the block as a stream.
    *
    * @param entries a stream to bucket
    * @param bucketTimestamps generate the bucket boundaries
    * @param maxNumberOfEntries per bock
    * @return a stream of blocks: TSEntry(stream-of-entries)
    */
  def bucketEntries[T](
      entries: Stream[TSEntry[T]],
      bucketTimestamps: Stream[Long],
      maxNumberOfEntries: Int
  ): Stream[TSEntry[Stream[TSEntry[T]]]] = {
    require(entries.nonEmpty, "Can't bucket an empty stream into blocks.")

    bucketEntries(bucketTimestamps, entries).flatMap {
      // The last tuple returned contains the end of validity of the last entry,
      // we therefore need it subsequently and can't let flatMap remove it.
      case (lastTs, Seq()) => Stream((lastTs, Stream()))

      // split buckets if they contain too may entries
      case (_, esInBucket) =>
        TimeSeries.groupEntries(esInBucket.toStream, maxNumberOfEntries)
    }.sliding(2)
      // use the next entry to define the validity of each entry
      // here we use the last tuple from above
      .map {
        case (ts, es) #:: (nextTs, _) #:: _ => TSEntry(ts, es, nextTs - ts)
      }
      .toStream
  }

  /**
    * Bucket the passed entries into sequences of entries that have domains delimited by the given
    * buckets. Individual entries will be split/trimmed wherever required.
    *
    * @param buckets generates the bucket boundaries
    * @param entries entries to be bucketed.
    * @return a stream of (bucket-start, bucket-entries), the last element will
    *         be (bucket-start, empty bucket) to define the end of duration
    */
  def bucketEntries[T](
      buckets: Stream[Long],
      entries: Seq[TSEntry[T]]
  ): Stream[(Long, Seq[TSEntry[T]])] =
    if (entries.isEmpty) {
      // When entries is empty, we do return one value with the current bucket's head and an empty time-series.
      (buckets.head, Seq.empty) +: Stream.empty
    } else {
      // Sanity check...
      require(
        buckets.head <= entries.head.timestamp,
        f"Bucket Stream MUST start at or before the first entry. First bucket was: ${buckets.head}, " +
          f"first entry timestamp was: ${entries.head.timestamp}"
      )

      // Sort out what's in the bucket and what definitely isn't
      entries.span(_.timestamp < buckets.tail.head) match {
        case (Seq(), _) =>
          // This bucket is empty
          (buckets.head, Seq.empty) #:: bucketEntries(buckets.tail, entries)
        case (within, next) =>
          // we have at least a single element within the bucket:
          // Gotta check for the case where it extends into the next bucket as well.
          // Add everything in the bucket except the last
          val (keep, nextBucket) = within.last.split(buckets.tail.head)
          (buckets.head, within.dropRight(1) ++ keep.entries) #::
            bucketEntries(buckets.tail, nextBucket.entries ++ next)
      }
    }

  /**
    * Bucket the passed entries into time-series that have domains delimited by
    * the given buckets. Individual entries will be split/trimmed wherever required.
    *
    * @param buckets generates the bucket boundaries
    * @param entries entries to be bucketed.
    * @param builder that will construct all the resulting timeseries
    * @return a stream of (bucket-start, corresponding-time-series)
    */
  def bucketEntriesToTimeSeries[T](
      buckets: Stream[Long],
      entries: Seq[TSEntry[T]],
      builder: => TimeSeriesBuilder[T]
  ): Stream[(Long, TimeSeries[T])] = {
    // Reusing the builder instance so we don't recreate one for each bucket
    // TODO: actually check if this makes any sense from a perf point of view?
    val b = builder
    bucketEntries(buckets, entries).map { tup =>
      // Better keep that map single threaded ;)
      b.clear()
      b ++= tup._2
      (tup._1, b.result())
    }
  }
}
