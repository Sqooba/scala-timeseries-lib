package io.sqooba.oss.timeseries

import java.nio.ByteBuffer

package object archive {

  /**
    * Represents a Gorilla encoded series of (timestamp, value) tuples, without validities.
    * It is just an array of bytes.
    */
  type GorillaArray = Array[Byte]

  /**
    * A block of a timeseries encoded according to the Gorilla TSC format to two byte arrays.
    *
    * @param valueBytes encodes the timeseries formed by the values along with their timestamps
    * @param validityBytes encodes the series formed by the validities with their timestamps
    */
  case class GorillaBlock(valueBytes: GorillaArray, validityBytes: GorillaArray)

  /**
    * Helper function
    * @param longs in an array
    * @return the same array but as an array of bytes
    */
  private[archive] def longArray2byteArray(longs: Array[Long]): Array[Byte] =
    longs
      .foldLeft(ByteBuffer.allocate(java.lang.Long.BYTES * longs.length))(
        (buffer, long) => buffer.putLong(long)
      )
      .array()

  /**
    * Helper function
    * @param bytes in an array
    * @return the same array but as an array of longs
    */
  private[archive] def byteArray2longArray(bytes: Array[Byte]): Array[Long] = {
    val buffer = ByteBuffer.wrap(bytes)

    Array.fill(bytes.length / java.lang.Long.BYTES) { buffer.getLong }
  }

  private[archive] def int2byteArray(int: Int): Array[Byte] =
    ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(int).array()
}
