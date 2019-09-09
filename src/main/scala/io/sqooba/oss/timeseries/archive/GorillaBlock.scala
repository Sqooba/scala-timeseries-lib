package io.sqooba.oss.timeseries.archive

import io.sqooba.oss.timeseries.immutable.TSEntry
import fi.iki.yak.ts.compression.gorilla._
import io.sqooba.oss.timeseries.TSEntryFitter

import scala.collection.mutable
import scala.util.Success

/** A GorillaBlock represents a compressed/encoded TimeSeries as defined in this
  * library.
  *
  * @param valueBytes encodes the timeseries formed by the values along with their timestamps
  * @param validityBytes encodes the series formed by the validities with their timestamps
  */
case class GorillaBlock(valueBytes: GorillaArray, validityBytes: GorillaArray)

object GorillaBlock {

  /**
    * Compresses all the entries of the stream according to the Gorilla TSC format to
    * two GorillaArrays and wraps them in a GorillaBlock.
    *
    * @note All of the entries in the stream should be able to fit in a single block.
    *       (I.e. no two entries should be further apart in time than 24h.)
    *
    * @param entries a non-empty stream of TSEntry[Double] to be compressed,
    *        other types of numbers need to be converted to doubles.
    * @return a gorilla encoded block
    */
  def compress(entries: Stream[TSEntry[Double]]): GorillaBlock =
    entries.foldLeft(new GorillaBlock.Builder())(_ += _).result()

  /**
    * Decompresses a Gorilla encoded timeseries block to a lazily evaluated stream
    * of TSEntries.
    *
    * @param block the compressed block containing both byte arrays
    * @return a stream of TSEntry[Double]
    */
  def decompress(block: GorillaBlock): Stream[TSEntry[Double]] = {

    // The underlying library throws IndexOutOfBounds, if something is not in
    // the expected format. We wrap that in a Try to return a custom error.
    val valueDecompressor    = wrapTryDecompressor(block.valueBytes)
    val validityDecompressor = wrapTryDecompressor(block.validityBytes)

    // lazily generates the stream of entries, pair by pair
    def nextEntry(): Stream[TSEntry[Double]] =
      (
        valueDecompressor.map(_.readPair()),
        validityDecompressor.map(_.readPair())
      ) match {
        // both timeseries have a next entry with equal timestamps
        case (Success(vPair: Pair), Success(dPair: Pair)) if vPair.getTimestamp == dPair.getTimestamp =>
          TSEntry(vPair.getTimestamp, vPair.getDoubleValue, dPair.getLongValue) #:: nextEntry()

        // end of both compressed timeseries
        case (Success(null), Success(null)) => Stream.empty

        case _ =>
          throw new IllegalArgumentException(s"The passed byte arrays are not valid compressed timeseries.")
      }

    nextEntry()
  }

  /**
    * A Scala 'mutable.Builder' for the iterative construction of a GorillaBlock. The
    * builder takes TSEntries and continually compresses them to gorilla arrays that
    * result in a GorillaBlock.
    *
    * @note This builder does no sanity checks on the TSEntries: you may use the
    *       TSEntryFitter for this or construct an actual TimeSeries.
    */
  class Builder(compress: Boolean = true) extends mutable.Builder[TSEntry[Double], GorillaBlock] {

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
    override def clear(): Unit = {
      valueOutput = new LongArrayOutput()
      validityOutput = new LongArrayOutput()
      valueCompressor = null
      validityCompressor = null

      entryBuilder.clear()
      resultCalled = false
    }

    override def +=(entry: TSEntry[Double]): this.type = {
      // If this is the first element added, initialise the compressors with its timestamp.
      if (Option(valueCompressor).isEmpty || Option(validityCompressor).isEmpty) {
        valueCompressor = new GorillaCompressor(entry.timestamp, valueOutput)
        validityCompressor = new GorillaCompressor(entry.timestamp, validityOutput)
      }

      entryBuilder.addAndFitLast(entry).foreach(compressEntry)
      this
    }

    private def compressEntry(entry: TSEntry[Double]): Unit = {
      valueCompressor.addValue(entry.timestamp, entry.value)
      validityCompressor.addValue(entry.timestamp, entry.validity)

    }

    def lastEntry: Option[TSEntry[Double]] = entryBuilder.lastEntry

    def isDomainContinuous: Boolean = entryBuilder.isDomainContinuous

    override def result(): GorillaBlock = {
      if (resultCalled) {
        throw new IllegalStateException("Cannot call result more than once, unless the builder was cleared.")
      } else if (Option(valueCompressor).isEmpty || Option(validityCompressor).isEmpty) {
        throw new IllegalStateException("Cannot call result if no element was added.")
      }
      resultCalled = true

      entryBuilder.lastEntry.foreach(compressEntry)
      valueCompressor.close()
      validityCompressor.close()

      GorillaBlock(
        longArray2byteArray(valueOutput.getLongArray),
        longArray2byteArray(validityOutput.getLongArray)
      )
    }
  }
}
