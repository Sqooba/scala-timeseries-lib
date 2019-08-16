package io.sqooba.oss.timeseries.archive

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import io.sqooba.oss.timeseries.TimeSeries
import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.immutable.SortedMap

/**
  * Binary format for storing a TimeSeries
  *
  * This object can write a TimeSeries composed of many Gorilla encoded blocks to a
  * stream/file. It uses a sequential layout with an index to permit quick look-up of
  * individual blocks by timestamp.
  *
  * Format
  * +---+---------+------------+---+----------+ ... +---------+--------+---+---+
  * | L | block 1 | block 1    | L | block 2  |     | block N | index  | L | V |
  * | 1 | values  | validities | 2 | values   |     | validi. | block  | I | N |
  * +---+---------+------------+---+----------+ ... +---------+--------+---+---+
  *
  * LI: length of index, VN: version number
  *
  * The Gorilla blocks are lined up one after another, ordered by time. They all have
  * different lengths. The Gorilla array with the values is always preceding the
  * Gorilla array of the validities. Before each Gorilla block there are 4 bytes
  * that contain the length of the value array as an Int. This serves to split the
  * block into the two arrays to decode.
  *
  * At the end of the stream there is an additional Gorilla array containing the
  * index that maps the start timestamp of each Gorilla block to its offset (in
  * bytes) from the beginning of the stream. The offset points to the value-array
  * length bytes.
  *
  * The length of this Gorilla array is written as an Int to the last 5-8 bytes. The
  * last 1-4 bytes contain the version number of the binary format as an Int. In that
  * manner a user can first read the footer, decode the index and then do fast
  * look-ups for blocks by timestamp.
  */
object TSBinaryFormat {

  /**
    * Specifies the version number of this binary format
    */
  val VERSION_NUMBER: Int = 0

  /**
    * Writes the provided TSEntries of Gorilla blocks to the provided output stream
    * according to the format specified by this object.
    *
    * @param buckets well-formed stream of entries containing Gorilla blocks
    * @param output the stream to write to
    */
  def write(buckets: Stream[TSEntry[GorillaBlock]], output: OutputStream): Unit = {
    require(buckets.nonEmpty, "At least one bucket to write needs to be provided.")

    // Construct a map of (timestamps, offset of encoded block in output) and
    // at the same time write the blocks.
    val (indexMap, lastOffset, lastTs) = buckets
      .foldLeft(SortedMap.empty[Long, Long], 0L, 0L) {
        // (the current index, the offset of the block)

        case ((map, offset, _), TSEntry(ts, GorillaBlock(valueBytes, validityBytes), validity)) =>
          // Write the length of the value array
          output.write(int2byteArray(valueBytes.length))

          // Write gorilla arrays consecutively on the stream.
          output.write(valueBytes)
          output.write(validityBytes)

          // Update the index.
          val lengthOfBlock = valueBytes.length + validityBytes.length + java.lang.Integer.BYTES
          val nextTs        = ts + validity
          (map + (ts -> offset), offset + lengthOfBlock, nextTs)
      }

    // Write index with the last entry pointing to the start of the index and therefore the end of the last block
    val compressedIndex = TSCompressor.compressTimestampTuples(indexMap + (lastTs -> lastOffset))
    output.write(compressedIndex)

    output.write(int2byteArray(compressedIndex.length))
    output.write(int2byteArray(VERSION_NUMBER))
    output.close()
  }

  /**
    * Reads and decompresses the index of a given seekable channel. The index must be
    * stored as a footer like it is described by TSBinaryFormat.
    *
    * @param channel a seekable byte channel
    * @return the index as a sorted map of (timestamp -> byte-offset) tuples
    */
  def readIndex(channel: SeekableByteChannel): SortedMap[Long, Long] = {
    val footerIntLength = 2 * java.lang.Integer.BYTES
    require(channel.size() > footerIntLength, "The binary input is too short to contain a gorilla block.")

    // read index size and version number
    channel.position(channel.size() - footerIntLength)
    val intBuffer = ByteBuffer.allocate(footerIntLength)
    channel.read(intBuffer)
    intBuffer.flip()
    val indexLength   = intBuffer.getInt
    val versionNumber = intBuffer.getInt
    require(versionNumber == VERSION_NUMBER, "The binary input's version is not compatible.")

    // read index
    channel.position(channel.size() - footerIntLength - indexLength)
    val indexBuffer = ByteBuffer.allocate(indexLength)
    channel.read(indexBuffer)

    TSCompressor.decompressTimestampTuples(indexBuffer.array())
  }

  /**
    * Reads (but doesn't decompress) a gorilla block from the given seekable channel
    * that contains a file of the TSBinaryFormat. The offset and the length of the
    * entire block.
    *
    * @param channel a seekable byte channel
    * @param offset the start of the block as given by the index in the footer of the file
    * @param length the length of the block, i.e. the difference between the offset and the
    *               next block's offset
    * @return a gorilla encoded block of a timeseries
    */
  def readBlock(channel: SeekableByteChannel, offset: Long, length: Int): GorillaBlock = {
    channel.position(offset)
    val buffer = ByteBuffer.allocate(length)

    require(channel.read(buffer) == length, "There is not enough data in the provided channel.")

    buffer.flip()
    val valuesLength  = buffer.getInt
    val valueBytes    = Array.ofDim[Byte](valuesLength)
    val validityBytes = Array.ofDim[Byte](length - valuesLength - java.lang.Integer.BYTES)

    buffer.get(valueBytes)
    buffer.get(validityBytes)
    GorillaBlock(valueBytes, validityBytes)
  }
}
