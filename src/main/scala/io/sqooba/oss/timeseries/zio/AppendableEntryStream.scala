package io.sqooba.oss.timeseries.zio

import io.sqooba.oss.timeseries.immutable.TSEntry
import zio.stream._
import zio.{Queue, Task, UIO, ZIO}

class AppendableEntryStream[T](
    finalizedSink: Queue[Take[Nothing, TSEntry[T]]],
    val finalizedEntries: Stream[Nothing, TSEntry[T]],
    fitter: ZEntryFitter[T]
) {

  def +=(elem: TSEntry[T]): Task[Unit] = addOne(elem)

  def ++=(xs: Seq[TSEntry[T]]): Task[Unit] =
    ZIO.foreach(xs)(addOne).unit

  def addOne(elem: TSEntry[T]): Task[Unit] = {
    fitter.addAndFitLast(elem).flatMap {
      case Some(entry) =>
        finalizedSink.offer(Take.single(entry)).unit
      case None =>
        ZIO.unit
    }
  }

  /**
    * Appends the last entry present in the fitter (if any)
    * and emits a terminating 'Take' to the queue.
    *
    * No entries should subsequently be added.
    */
  def close(): UIO[Unit] =
    appendLastEntryIfRequired() *> finalizedSink.offer(Take.end).unit

  private def appendLastEntryIfRequired() =
    fitter.lastEntry.flatMap {
      case Some(e) =>
        finalizedSink.offer(Take.single(e)).unit
      case None =>
        ZIO.unit
    }
}

object AppendableEntryStream {

  def unbounded[T](compress: Boolean): UIO[AppendableEntryStream[T]] = {
    for {
      // Create an unbounded queue that receives Takes
      q <- Queue.unbounded[Take[Nothing, TSEntry[T]]]
      // Build a stream from the queue and do a `flattenTake` so we can
      // terminate a stream of entries by passing a Take.end to the queue
      // (Using #fromQueueWithShutdown() so the queue gets shutdown
      //  once the stream has received the terminating entry)
      s = Stream.fromQueueWithShutdown(q).flattenTake
      fitter <- ZEntryFitter.init[T](compress)
    } yield new AppendableEntryStream(q, s, fitter)
  }

}
