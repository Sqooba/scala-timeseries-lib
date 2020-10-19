package io.sqooba.oss.timeseries.archive

import fi.iki.yak.ts.compression.gorilla._
import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.validation.{TSEntryFitter, TimestampValidator}

import scala.util.Success

/** A GorillaBlock represents a compressed/encoded TimeSeries as defined in this
  * library. It is the unit of compression/decompression for series data.
  */
trait GorillaBlock {

  /** Decompress this Gorilla encoded timeseries block into a lazily evaluated
    * stream of TSEntries.
    *
    * @return a stream of TSEntry[Double]
    */
  def decompress: Stream[TSEntry[Double]]

  /** @return the bytes for storing this block in binary format. This can be
    * called multiple times.
    */
  def serialize: Array[Byte]
}

/** Standard implementation of the GorillaBlock that has one GorillaArray
  * for the values and one for the validities of the timeseries.
  *
  * @param valueBytes    encodes the timeseries formed by the values along with their timestamps
  * @param validityBytes encodes the series formed by the validities with their timestamps
  *
  * This implementation also defines a binary format for storing the two
  * GorillaArrays. The values array is always preceding the validities array.
  * Before the block there are 4 bytes that contain the length of the values
  * array as an Int. This serves to split the block into the two arrays at
  * decoding.
  */
case class TupleGorillaBlock private (
    valueBytes: GorillaArray,
    validityBytes: GorillaArray
) extends GorillaBlock {

  require(valueBytes.nonEmpty, "Value GorillaArray cannot be empty.")
  require(validityBytes.nonEmpty, "Validities GorillaArray cannot be empty.")

  def serialize: Array[Byte] =
    int2ByteArray(valueBytes.length) ++ valueBytes ++ validityBytes

  def decompress: Stream[TSEntry[Double]] = {

    // The underlying library throws IndexOutOfBounds, if something is not in
    // the expected format. We wrap that in a Try to return a custom error.
    val valueDecompressor    = wrapTryDecompressor(valueBytes)
    val validityDecompressor = wrapTryDecompressor(validityBytes)

    // lazily generates the stream of entries, pair by pair
    def nextEntry: Stream[TSEntry[Double]] =
      (
        valueDecompressor.map(_.readPair()),
        validityDecompressor.map(_.readPair())
      ) match {
        // both timeseries have a next entry with equal timestamps
        case (Success(vPair: Pair), Success(dPair: Pair)) if vPair.getTimestamp == dPair.getTimestamp =>
          TSEntry(vPair.getTimestamp, vPair.getDoubleValue, dPair.getLongValue) #:: nextEntry

        // end of both compressed timeseries
        case (Success(null), Success(null)) => Stream.empty

        case _ =>
          throw new IllegalArgumentException(
            s"The byte arrays in this block are not a valid compressed timeseries."
          )
      }

    nextEntry
  }
}

/**
  * GorillaBlock for series that have mostly similar validities. This can be
  * stored more efficiently with a single GorillaArray. After decompression all
  * entries will have the validity of the sample rate. This rate may include a
  * bit of margin for jitter because individual validities will be trimmed at if
  * they overlap once they are put into a 'TimeSeries'.
  *
  * @param valueBytes encodes the timeseries formed by the values along with their timestamps
  * @param sampleRate the maximal validity of each entry in the series after decompression
  */
case class SampledGorillaBlock private (
    valueBytes: GorillaArray,
    sampleRate: Long
) extends GorillaBlock {

  require(valueBytes.nonEmpty, "Value GorillaArray cannot be empty.")
  require(sampleRate > 0, "Sampling rate must be positive.")

  // the sample rate will be serialized by the GorillaSuperBlock
  def serialize: Array[Byte] = valueBytes

  def decompress: Stream[TSEntry[Double]] = {
    // The underlying library throws IndexOutOfBounds, if something is not in
    // the expected format. We wrap that in a Try to return a custom error.
    val decompressor = wrapTryDecompressor(valueBytes)

    // lazily generates the stream of entries, pair by pair
    def nextEntry: Stream[TSEntry[Double]] =
      decompressor.map(_.readPair()) match {
        case Success(pair: Pair) =>
          TSEntry(pair.getTimestamp, pair.getDoubleValue, sampleRate) #:: nextEntry

        case Success(null) => Stream.empty
        case _ =>
          throw new IllegalArgumentException(
            s"The byte array in this block isn't a valid compressed timeseries."
          )
      }

    nextEntry
  }
}

object GorillaBlock {

  /** Create a GorillaBlock from a tuple of GorillaArrays.
    *
    * @param valueBytes encodes the timeseries formed by the values along with their timestamps
    * @param validityBytes encodes the series formed by the validities with their timestamps
    */
  def fromTupleArrays(valueBytes: GorillaArray, validityBytes: GorillaArray): GorillaBlock =
    TupleGorillaBlock(valueBytes, validityBytes)

  /** Create a GorillaBlock from a value GorillaArray and a sample rate.
    *
    * @param valueBytes encodes the timeseries formed by the values along with their timestamps
    * @param sampleRate the constant validity of each entry in the series
    */
  def fromSampled(valueBytes: GorillaArray, sampleRate: Long): GorillaBlock =
    SampledGorillaBlock(valueBytes, sampleRate)

  /** Deserialize a tuple GorillaBlock according to the binary format specified
    * above and return it.
    */
  def fromTupleSerialized(bytes: Array[Byte]): GorillaBlock = {
    val (lengthBytes, arrayBytes)   = bytes.splitAt(Integer.BYTES)
    val (valueBytes, validityBytes) = arrayBytes.splitAt(byteArray2Int(lengthBytes))

    TupleGorillaBlock(valueBytes, validityBytes)
  }

  /** Compress all the entries of the stream according to the Gorilla TSC format to
    * two GorillaArrays and wrap them in a GorillaBlock.
    *
    * @note The entries need to be a well-formed series according to
    *       TSEntryFitter and TimestampValidator. Those constraints are checked
    *       and enforced.
    *
    * @param entries a non-empty stream of TSEntry[Double] to be compressed,
    *        other types of numbers need to be converted to doubles
    * @return a gorilla encoded block
    */
  def compress(entries: Stream[TSEntry[Double]]): GorillaBlock =
    entries.foldLeft(GorillaBlock.newBuilder)(_ += _).result()

  /** Compress all the entries of the stream according to the Gorilla TSC format to
    * one GorillaArray and wrap it in a GorillaBlock. The sample rate will be written
    * as well.
    *
    * @note The entries need to be a well-formed series according to
    *       TSEntryFitter and TimestampValidator. Those constraints are checked
    *       and enforced.
    *
    * @param entries a non-empty stream of TSEntry[Double] to be compressed,
    *        their validities will be discarded
    * @param sampleRate the fixed validity for all the entries
    * @return a gorilla encoded block
    */
  def compressSampled(entries: Stream[TSEntry[Double]], sampleRate: Long): GorillaBlock =
    entries.foldLeft(GorillaBlock.newBuilder(sampleRate))(_ += _).result()

  /** A 'mutable.Builder' for the iterative construction of a GorillaBlock. The
    * builder takes TSEntries and continually encodes them. This also compresses
    * contiguous equal entries.
    */
  def newBuilder: Builder = newBuilder(true)

  /** A 'mutable.Builder' for the iterative construction of a GorillaBlock. The
    * builder takes TSEntries and continually encodes them.
    */
  def newBuilder(compress: Boolean): Builder = new Builder(None, compress)

  /** A 'mutable.Builder' for the iterative construction of a GorillaBlock. The
    * builder takes TSEntries and continually encodes them to a sampled
    * GorillaBlock in which all entries have the validity of the sample rate.
    * This also compresses contiguous equal entries.
    */
  def newBuilder(fixedValidity: Long): Builder =
    new Builder(Some(fixedValidity), compress = true)

  /** A 'mutable.Builder' for the iterative construction of a GorillaBlock. The
    * builder takes TSEntries and continually encodes them to a sampled
    * GorillaBlock in which all entries have the validity of the sample rate.
    */
  def newBuilder(fixedValidity: Long, compress: Boolean): Builder =
    new Builder(Some(fixedValidity), compress)

  /** A 'mutable.Builder' for the iterative construction of a GorillaBlock. */
  class Builder private[GorillaBlock] (
      validity: Option[Long],
      compress: Boolean
  ) {

    require(validity.forall(_ > 0), "Sampling rate must be positive.")

    // These need be vars because the Java implementations don't provide clear() methods.
    private var valueOutput: LongArrayOutput          = _
    private var valueCompressor: GorillaCompressor    = _
    private var validityOutput: LongArrayOutput       = _
    private var validityCompressor: GorillaCompressor = _

    private val entryBuilder = new TSEntryFitter[Double](compress)
    private var resultCalled = false

    clear()

    // Reset the builder to its initial state. The compressors must be set to null,
    // because they rely on the first timestamp which is only available at the
    // first addition of an element.
    def clear(): Unit = {
      valueOutput = new LongArrayOutput()
      validityOutput = new LongArrayOutput()
      valueCompressor = null
      validityCompressor = null

      entryBuilder.clear()
      resultCalled = false
    }

    def +=(entry: TSEntry[Double]): this.type = addOne(entry)

    def addOne(entry: TSEntry[Double]): this.type = {
      // If this is the first element added, initialise the compressors with its timestamp.
      if (lastEntry.isEmpty) {
        // NOTE: Don't forget to validate the first timestamp, if a block timestamp
        // other than the first entry's timestamp is used.
        valueCompressor = new GorillaCompressor(entry.timestamp, valueOutput)
        validityCompressor = new GorillaCompressor(entry.timestamp, validityOutput)
      } else {
        TimestampValidator.validateGorilla(lastEntry.get.timestamp, entry.timestamp)
      }

      entryBuilder.addAndFitLast(entry).foreach(compressEntry)
      this
    }

    private def compressEntry(entry: TSEntry[Double]): Unit = {
      valueCompressor.addValue(entry.timestamp, entry.value)
      validityCompressor.addValue(entry.timestamp, entry.validity)
    }

    /** @return the last entry that was added to the fitter. This entry can still change
      *         if more entries are added (it might be compressed/trimmed).
      */
    def lastEntry: Option[TSEntry[Double]] = entryBuilder.lastEntry

    /** @return whether all added entries so far were either contiguous or overlapping.
      *         I.e. there were no holes in the domain of definition of the entries seen so far.
      */
    def isDomainContinuous: Boolean = entryBuilder.isDomainContinuous

    def result(): GorillaBlock = {
      if (resultCalled) {
        throw new IllegalStateException(
          "Cannot call result more than once, unless the builder was cleared."
        )
      } else if (lastEntry.isEmpty) {
        throw new IllegalStateException("Cannot call result if no element was added.")
      }
      resultCalled = true

      entryBuilder.lastEntry.foreach(compressEntry)
      valueCompressor.close()
      validityCompressor.close()

      // Choose the sampled GorillaBlock implemenation over the tuple one if
      // a fixed validity is given.
      validity match {
        case Some(fixedValidity) =>
          SampledGorillaBlock(
            longArray2ByteArray(valueOutput.getLongArray),
            fixedValidity
          )
        case None =>
          TupleGorillaBlock(
            longArray2ByteArray(valueOutput.getLongArray),
            longArray2ByteArray(validityOutput.getLongArray)
          )
      }
    }
  }
}
