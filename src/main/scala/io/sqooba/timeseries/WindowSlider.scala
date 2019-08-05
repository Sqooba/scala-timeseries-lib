package io.sqooba.timeseries

import io.sqooba.timeseries.immutable.TSEntry

import scala.collection.immutable.Queue

object WindowSlider {

  type Window[T] = TSEntry[Queue[TSEntry[T]]]

  /**
    * Slide a window of size 'windowWidth' on the entries present in 'in'.
    *
    * In the returned stream, for every entry 'E' with timestamp ts_e and validity d_e,
    * the content of that entry can be interpreted as:
    *   "All entries from the original series that are within t-window and t, for t in [ts_e, ts_e + d_e[".
    *
    * The returned stream can be seen as a time-series that, when queried for time 't', answers the question
    * "All the entries that have a domain that is at least partly contained in 't-window' and 't'.
    *
    * @param in
    * @param windowWidth
    * @tparam T
    * @return
    */
  def window[T](
      in: Stream[TSEntry[T]],
      windowWidth: Long
  ): Stream[Window[T]] = {
    require(windowWidth > 0, "Needs a strictly positive window size")
    if (in.isEmpty) {
      Stream.Empty
    } else {
      windowRec(
        in,
        Queue.empty,
        in.head.timestamp,
        windowWidth
      )
    }
  }

  /**
    * Rough outline of the way the windowing works:
    *   - initialise the recursion with the entire remaining stream,
    *     an empty 'previous' queue and the 'cursor' pointing
    *     to the first entry
    *
    *   - determine whether we need to:
    *     - add an entry to the window content, and/or
    *     - remove an entry from the window content
    *   - determine:
    *     - the time until the next addition, if any
    *     - the time until the next removal
    *   (If the current window is empty, the next removal may be the value that will be added.)
    *   - the next cursor will be the minimum between the two values above
    *   - recurse
    *
    * @param remaining the entries over which the window still needs to pass
    * @param previousEntryContent the content of the window at the last iteration
    * @param timeCursor points to 'where we are' on the timeline'
    * @param windowWidth width of the window
    * @return a stream of entries that contain the content of the window for their domain of definition.
    */
  private def windowRec[T](
      remaining: Stream[TSEntry[T]],
      previousEntryContent: Queue[TSEntry[T]],
      timeCursor: Long,
      windowWidth: Long
  ): Stream[Window[T]] = {

    val (fromRemaining, dropFromWindow, advance) =
      whatToUpdate(
        remaining,
        previousEntryContent,
        timeCursor,
        windowWidth
      )

    if (advance == 0) {
      // This signals the end of the recursion
      return Stream.empty
    }

    val nextCursor  = timeCursor + advance
    val newValidity = nextCursor - timeCursor

    (fromRemaining, dropFromWindow) match {
      case (true, true) =>
        val newWindow = TSEntry(
          timeCursor,
          previousEntryContent.tail :+ remaining.head,
          newValidity
        )
        newWindow #:: windowRec(remaining.tail, newWindow.value, nextCursor, windowWidth)
      case (true, false) =>
        val newWindow = TSEntry(
          timeCursor,
          previousEntryContent :+ remaining.head,
          newValidity
        )
        newWindow #:: windowRec(remaining.tail, newWindow.value, nextCursor, windowWidth)
      case (false, true) =>
        val newWindow = TSEntry(
          timeCursor,
          previousEntryContent.tail,
          newValidity
        )
        newWindow #:: windowRec(remaining, newWindow.value, nextCursor, windowWidth)
      case _ =>
        throw new IllegalStateException(
          "Something went very wrong. Please file a bug report. Would you fancy a cup of tea while we fix this?"
        )
    }

  }

  /**
    * Tells us if we can take from the remaining, if we need to remove something from the window, or both,
    * and by how much we can advance the cursor.
    *
    * The cursor should always be located either on the timestamp of the next entry to be added,
    * or on (e_definedUntil + windowWidth) with 'e' being the next entry to be removed.
    * For the last iteration, the cursor will be set to the end of the domain of the last entry of
    * the series being windowed.
    *
    * Notes:
    *  - windows are begin-inclusive and end-exclusive: [begin, end[
    *  - once there are no more remaining entries, and the cursor has reached the end of the last added entry,
    *    'advance-by' will always be 0. This is because we do not slide the window into the 'future' beyond
    *    the last entry's domain of definition.
    *
    * @param remaining      the stream of remaining entries over which to slide a window
    * @param previousBucket the content of the previous bucket
    * @param timeCursor         the location of the current cursor on the timeline
    * @param windowWidth    width of the sliding window.
    * @return a tuple ('take-from-remaining', 'remove-from-window', 'advance-by')
    */
  private[timeseries] def whatToUpdate(
      remaining: Stream[TSEntry[_]],
      previousBucket: Queue[TSEntry[_]],
      timeCursor: Long,
      windowWidth: Long
  ): (Boolean, Boolean, Long) = {

    // Using an assert, as this triggering would indicate a bug in the recursion logic.
    // TODO: use relevant build/run flags to not have asserts at runtime ;)
    assert(remaining.nonEmpty || previousBucket.nonEmpty, "Can't accept empty remaining and empty previous window.")
    assert(
      remaining.headOption
        .fold(false)(e => e.timestamp == timeCursor)
      || previousBucket.headOption.fold(false)(_.definedUntil == timeCursor - windowWidth)
      || previousBucket.lastOption.fold(false)(_.definedUntil == timeCursor),
      "cursor MUST be sitting on the timestamp of the first remaining," +
        "the end of validity of the first element in the queue, or the end of validity of the last entry in the queue"
    )

    // Cursor reached the end of the domain of definition of the timeseries.
    if (remaining.isEmpty && previousBucket.last.definedUntil == timeCursor) {
      // See notes in doc. Currently we don't want to slide the window further
      // when there are no more to be added.
      return (false, false, 0)
    }

    // Determine what kind of updates we need to do to the window content
    val takeFromRemaining =
      remaining.headOption
        .map(_.timestamp - timeCursor == 0)
        .getOrElse(false)

    // We need to remove an entry from the elements in the sliding window once their 'definedUntil' is about to be
    // equal to the window tail
    val removeFromWindow =
      previousBucket.headOption
        .map(_.definedUntil - (timeCursor - windowWidth) == 0)
        .getOrElse(false)

    // Determine by how much we can advance the window:
    // (what we must add to the cursor so it is equal to the next addition's timestamp)
    val (spaceUntilNextAddition) = remaining match {
      case head #:: tail =>
        if (takeFromRemaining) {
          // If we are taking from the remaining now, the next one to be added is the first of the remaining entries.
          // If no more entries are remaining, the cursor should only be advanced to the end of the last entry's domain.
          // (This allows the termination condition to trigger, and ensure we don't slide the window beyond the domain
          // of definition of the input.)
          tail.headOption.fold(head.validity)(_.timestamp - timeCursor)
        } else {
          head.timestamp - timeCursor
        }
      case _ => Long.MaxValue // Nothing to add until infinity...
    }

    // Time until the next removal, from the present cursor
    // (what we must add to the cursor so (cursor - windowWidth) is equal to the next removal's 'definedUntil')
    val spaceUntilNextRemoval = if (removeFromWindow) {
      previousBucket.tail.headOption
        .orElse(remaining.headOption)
        .fold(Long.MaxValue)(_.definedUntil) - (timeCursor - windowWidth)
    } else {
      previousBucket.headOption
        .fold(remaining.head.definedUntil)(_.definedUntil) - (timeCursor - windowWidth)
    }

    val advance = Math.min(spaceUntilNextAddition, spaceUntilNextRemoval)

    // Kinda cumbersome way to check that we don't advance the cursor beyond the last entry's validity after
    // we added the last element to it but still need to remove previous entries while we approach the end of the series.
    val nextAdvance = if (remaining.nonEmpty) {
      advance
    } else {
      Math.min(advance, previousBucket.last.definedUntil - timeCursor)
    }

    assert(takeFromRemaining || removeFromWindow, "Looks like a bug, mate...")
    (takeFromRemaining, removeFromWindow, nextAdvance)
  }

}
