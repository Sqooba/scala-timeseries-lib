package io.sqooba.oss.timeseries

import java.nio.ByteBuffer

import fi.iki.yak.ts.compression.gorilla.{GorillaDecompressor, LongArrayInput}

import scala.util.Try

/** Provides abstraction and tools for compressing/archiving time series data. The
  * compression used is Gorilla TSC encoding implemented by the Java library
  * [[fi.iki.yak.ts.compression.gorilla]].
  *
  * These are the main types:
  *   - [[io.sqooba.oss.timeseries.archive.GorillaArray]]:
  *     a simple byte array that represents a compressed/encoded map of
  *     (timestamp, value) tuples.
  *
  *   - [[io.sqooba.oss.timeseries.archive.GorillaBlock]]:
  *     the representation of a compressed/encoded TimeSeries as defined in this
  *     library.
  *
  *   - [[io.sqooba.oss.timeseries.archive.GorillaSuperBlock]]:
  *     groups many GorillaBlocks of the same TimeSeries into a larger binary
  *     format. This allows compression of a timeseries that spans a very long
  *     range of time.
  *
  *   - [[io.sqooba.oss.timeseries.archive.MultiSeriesBlock]]:
  *     a large block of many indexed GorillaSuperBlocks. This format can be
  *     used to store all the data of a certain time period into one single
  *     binary blob.
  *
  * @note The only supported type for the values of the TSEntries at the moment is
  * Double. This can lead to precision problems if you have very high long  values
  * that you convert to double and pass to the compression.
  */
// TODO make compression generic for all numeric types. This should be possible because
//   all numeric types can be represented by the 64-bits of a Long.
//   Tracked by T547.
package object archive {

  /** Represents a gorilla encoded series of (timestamp, value) tuples, without validities.
    * It is just an array of bytes.
    */
  type GorillaArray = Array[Byte]

  /** Helper function
    * @param longs in an array
    * @return the same array but as an array of bytes
    */
  private[archive] def longArray2ByteArray(longs: Array[Long]): Array[Byte] =
    longs
      .foldLeft(ByteBuffer.allocate(java.lang.Long.BYTES * longs.length))(
        (buffer, long) => buffer.putLong(long)
      )
      .array()

  /** Helper function
    * @param bytes in an array
    * @return the same array but as an array of longs
    */
  private[archive] def byteArray2LongArray(bytes: Array[Byte]): Array[Long] = {
    val buffer = ByteBuffer.wrap(bytes)
    Array.fill(bytes.length / java.lang.Long.BYTES) { buffer.getLong }
  }

  private[archive] def int2ByteArray(int: Int): Array[Byte] =
    int2ByteBuffer(int).array()

  private[archive] def int2ByteBuffer(int: Int): ByteBuffer = {
    val buffer = ByteBuffer.allocate(Integer.BYTES).putInt(int)
    buffer.flip()
    buffer
  }

  private[archive] def byteArray2Int(array: Array[Byte]): Int =
    ByteBuffer.wrap(array).getInt

  private[archive] def wrapTryDecompressor(bytes: GorillaArray): Try[GorillaDecompressor] =
    Try(new GorillaDecompressor(new LongArrayInput(byteArray2LongArray(bytes))))

}
