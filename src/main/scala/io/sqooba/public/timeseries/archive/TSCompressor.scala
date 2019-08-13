package io.sqooba.public.timeseries.archive

import fi.iki.yak.ts.compression.gorilla.{GorillaDecompressor, LongArrayInput, Pair}
import io.sqooba.public.timeseries.immutable.TSEntry

import scala.util.{Success, Try}

/**
  * A basic wrapper around the 'gorilla-tsc' Java library. Streams of TSEntries can
  * be compressed and decompressed with the Gorilla TSC encoding implemented by the library.
  * As this format only has tuples of a timestamp and a value, this object outputs two
  * compressed timeseries for encoding the triples of TSEntry (see #compress).
  *
  * @note The only supported type at the moment is Double. This can lead to precision
  *       problems if you have very high long  values that you convert to double and pass
  *       to the compression.
  */
// TODO make compression generic for all numeric types. This should be possible because
//   all numeric types can be represented by the 64-bits of a Long.
//   Tracked by T547.
object TSCompressor {

  /**
    * A block of a timeseries encoded according to the Gorilla TSC format to two byte arrays.
    *
    * @param valueBytes encodes the timeseries formed by the values along with their timestamps
    * @param validityBytes encodes the series formed by the validities with their timestamps
    */
  case class GorillaBlock(valueBytes: Array[Byte], validityBytes: Array[Byte])

  /**
    * Compresses all the entries of the stream according to the Gorilla TSC format
    *  to two byte arrays. See type CompressedBlock for more details on the
    *  resulting byte arrays.
    *
    * @note All of the entries in the stream should be able to fit in a single TSC block.
    *       (No two entries should be further apart in time than 24h.)
    *
    * @param entries a non-empty stream of TSEntry[Double] to be compressed,
    *        other types of numbers need to be converted to doubles.
    *
    * @return a gorilla encoded block
    */
  def compress(entries: Stream[TSEntry[Double]]): GorillaBlock =
    if (entries.isEmpty) {
      throw new IllegalArgumentException("The stream to compress needs to contain at least one TSEntry.")
    } else {
      entries.foldLeft(new GorillaBlockBuilder())(_ += _).result()
    }

  /**
    * Decompresses a Gorilla TSC encoded timeseries block to a lazily evaluated stream
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

  private def wrapTryDecompressor(bytes: Array[Byte]): Try[GorillaDecompressor] =
    Try(new GorillaDecompressor(new LongArrayInput(byteArray2longArray(bytes))))

}
