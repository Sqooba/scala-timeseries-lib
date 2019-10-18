package io.sqooba.oss.timeseries.validation

/**
  * This object specifies and checks all the different types of constraints that
  * are imposed on timestamps of timeseries.
  * There are two main types of constraints:
  *
  *  - logical constraints: chronological, increasing order
  *  - compression constraints: Positivity, and maximal differences. These are
  *    (implicitly) given by the Gorilla library but the library itself does no
  *    checks. Therefore, we perform them here.
  */
object TimestampValidator {

  /** Maximal timestamp difference between the timestamp in the header of
    * a gorilla array and the first entry's timestamp.
    *
    * This is due to the fact that the library uses 27 bits and ZigZag encoding
    * to encode this difference.
    */
  val MaxGapToBlock: Long = Integer.parseInt("1" * 27, 2).toLong - 1

  /** Maximal timestamp difference for two consecutive entries in a gorilla
    * compressed series.
    *
    * This is due to the fact that the difference is cast to a Java Integer inside
    * the library.
    */
  val MaxGap: Long = Int.MaxValue.toLong

  /** Validates a sequence of consecutive timestamps of a timeseries for logical AND
    * gorilla compression constraints. Throws if constraints are violated.
    *
    * @param seqTs a sequence of at least 2 timestamps
    */
  def validateGorilla(seqTs: Seq[Long]): Unit = {
    require(seqTs.nonEmpty)
    seqTs.sliding(2).foreach(pair => validateGorilla(pair.head, pair.last))
  }

  /** Validates a sequence of consecutive timestamps of a timeseries for logical constraints
    * i.e. chronological order. Throws if constraints are violated.
    *
    * @param seqTs a sequence of at least 2 timestamps
    */
  def validate(seqTs: Seq[Long]): Unit = {
    require(seqTs.nonEmpty)
    seqTs.sliding(2).foreach(pair => validate(pair.head, pair.last))
  }

  /** Validates two consecutive timestamps of a timeseries for logical AND
    * gorilla compression constraints. Throws if constraints are violated.
    */
  def validateGorilla(lastTs: Long, currentTs: Long): Unit = {
    validate(lastTs, currentTs)

    requirePositive(lastTs)
    requirePositive(currentTs)
    require(
      currentTs < lastTs + MaxGap,
      s"Timestamps cannot have a difference larger than $MaxGap, was $currentTs - $lastTs = ${currentTs - lastTs}."
    )
  }

  /** Validates the timestamp written in the header of a gorilla block and the
    * first entry's timestamp of the timeseries. Checks for logical AND
    * gorilla compression constraints. Throws if constraints are violated.
    */
  def validateGorillaFirst(blockTs: Long, firstEntryTs: Long): Unit = {
    requirePositive(blockTs)
    requirePositive(firstEntryTs)

    require(
      blockTs <= firstEntryTs,
      s"The block timestamp cannot come after the first entry's timestamp, was $blockTs before $firstEntryTs."
    )

    require(
      firstEntryTs < blockTs + MaxGapToBlock,
      s"The first entry's timestamp must be smaller than the block timestamp ($blockTs) + $MaxGapToBlock, was $firstEntryTs."
    )
  }

  /** Validates two consecutive timestamps of a timeseries for logical constraints
    * i.e. chronological order. Throws if constraints are violated.
    */
  def validate(lastTs: Long, currentTs: Long): Unit = {
    require(
      lastTs < currentTs,
      s"The timestamps need to be strictly increasing, was $lastTs before $currentTs."
    )
  }

  private def requirePositive(ts: Long): Unit =
    require(ts > 0, s"Timestamps must be positive, was $ts.")
}
