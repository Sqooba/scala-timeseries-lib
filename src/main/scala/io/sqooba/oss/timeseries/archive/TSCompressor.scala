package io.sqooba.oss.timeseries.archive

import io.sqooba.oss.timeseries.immutable.TSEntry
import fi.iki.yak.ts.compression.gorilla._

import scala.collection.immutable.SortedMap
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

  /**
    * Compresses a map of timestamps to longs. This is usually a raw timeseries
    * without the validities that are defined by TimeSeries of this library.
    *
    * @param map sorted tuples
    * @return a gorilla encoded byte array
    */
  def compressTimestampTuples(map: SortedMap[Long, Long]): GorillaArray = {
    require(map.nonEmpty, "The map to compress needs to contain at least one element.")

    val output     = new LongArrayOutput()
    val compressor = new GorillaCompressor(map.head._1, output)

    map.foreach(elem => compressor.addValue(elem._1, elem._2))
    compressor.close()

    longArray2byteArray(output.getLongArray)
  }

  /**
    * Decompresses a map of timestamps to longs that was compressed by
    * 'compressTimestampTuples'.
    *
    * @param array a gorilla encoded byte array
    * @return a sorted raw timeseries as a map
    */
  def decompressTimestampTuples(array: GorillaArray): SortedMap[Long, Long] = {
    val decompressor = wrapTryDecompressor(array)

    // the appending converts the stream to a map
    SortedMap[Long, Long]() ++ Stream
      .continually(decompressor.map(_.readPair()))
      .takeWhile {
        case Success(_: Pair) => true
        case Success(null)    => false
        case _ =>
          throw new IllegalArgumentException(s"The passed byte array is not a valid compressed timeseries.")
      }
      .map(t => t.get.getTimestamp -> t.get.getLongValue)
  }

  private def wrapTryDecompressor(bytes: GorillaArray): Try[GorillaDecompressor] =
    Try(new GorillaDecompressor(new LongArrayInput(byteArray2longArray(bytes))))

}
