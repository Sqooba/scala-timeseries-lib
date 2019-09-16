package io.sqooba.oss.timeseries.archive

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import io.sqooba.oss.timeseries.immutable.TSEntry

import scala.collection.immutable.SortedMap
import scala.collection.mutable

/** A GorillaSuperBlock is a binary format for storing a long
  * [[io.sqooba.oss.timeseries.TimeSeries]] composed of many GorillaBlocks. It
  * uses a sequential layout with an index to permit quick look-up of individual
  * blocks by timestamp.
  *
  * GorillaSuperBlock Format
  * +-------+-------+ ... +----- -+----------+---+---+
  * | block | block |     | block | Index    | L | V |
  * | 1     | 2     |     | N     |          | F | N |
  * +-------+-------+ ... +-------+----------+---+---+
  *
  * LF: length of footer, VN: version number
  *
  * The Gorilla blocks are lined up one after another, ordered by timestamp.
  * They all have different lengths. These can be recovered from the offsets
  * stored in the footer.
  *
  * At the end of the stream there is an additional Gorilla array containing the
  * index that maps the start timestamp of each GorillaBlock to its offset from
  * the beginning of the stream (in bytes). The length of the index GorillaArray
  * is written as an Int to the last 5-8 bytes.
  *
  * The last 1-4 bytes contain the version number of the binary format as an
  * Int. In that manner a user can first read the footer, decode the index and
  * then do fast look-ups for blocks by timestamp.
  */
object GorillaSuperBlock {

  /**
    * Specifies the version number of this binary format
    */
  val VERSION_NUMBER: Int = 0

  /**
    * Writes the provided TSEntries of Gorilla blocks to the provided output stream
    * according to the GorillaSuperBlock format.
    *
    * @param buckets well-formed stream of entries containing Gorilla blocks
    * @param output the stream to write to
    */
  def write(buckets: Seq[TSEntry[GorillaBlock]], output: OutputStream): Unit =
    buckets.foldLeft(new GorillaSuperBlock.Writer(output))(_ += _).close()

  /**
    * Reads and decompresses the index of a given seekable channel. The index must be
    * stored as a footer like it is described by GorillaSuperBlock.
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

    GorillaArray.decompressTimestampTuples(indexBuffer.array())
  }

  /**
    * Reads (but doesn't decompress) a gorilla block from the given seekable channel
    * that contains a file of the GorillaSuperBlock. The offset and the length of the
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
    val bytes = Array.ofDim[Byte](length)
    buffer.get(bytes)

    GorillaBlock.fromTupleSerialized(bytes)
  }

  /** Helper function to read an entire GorillaSuperBlock. This uses 'readIndex'
    * and 'readBlock' internally.
    *
    * @param channel a seekable byte channel of the file
    * @return a sequence of gorilla blocks
    */
  def readAll(channel: SeekableByteChannel): Seq[TSEntry[GorillaBlock]] =
    readIndex(channel).toSeq
      .sliding(2)
      .map {
        case (ts, offset) :: (nextTs, nextOffset) :: _ =>
          TSEntry(
            ts,
            GorillaSuperBlock.readBlock(channel, offset, (nextOffset - offset).toInt),
            nextTs - ts
          )
      }
      .to(LazyList)

  class Writer(output: OutputStream) extends mutable.Growable[TSEntry[GorillaBlock]] with AutoCloseable {

    // A map of (timestamps -> offset of encoded block in output)
    private var index: SortedMap[Long, Long] = SortedMap.empty[Long, Long]

    private var lastLength: Long       = 0L
    private var lastDefinedUntil: Long = 0L

    private var resultCalled = false

    override def addOne(entry: TSEntry[GorillaBlock]): Writer.this.type = {
      val TSEntry(ts, gorillaBlock, _) = entry

      val blockBytes = gorillaBlock.serialize
      output.write(blockBytes)

      index += (ts -> (currentOffset + lastLength))
      lastLength = blockBytes.length
      lastDefinedUntil = entry.definedUntil

      this
    }

    override def clear(): Unit = {
      index = SortedMap.empty[Long, Long]
      lastLength = 0L
      lastDefinedUntil = 0L
      resultCalled = false
    }

    override def close(): Unit = {
      if (index.isEmpty) {
        throw new IllegalStateException("Cannot write a GorillaSuperBlock of zero GorillaBlocks.")
      } else if (resultCalled) {
        throw new IllegalStateException("Cannot call result more than once, unless the builder was cleared.")
      }

      resultCalled = true

      // Add a last entry to the index. It points to the end of the last block
      // i.e. serving as and end marker.
      index += (lastDefinedUntil -> (currentOffset + lastLength))

      val compressedIndex = GorillaArray.compressTimestampTuples(index)
      output.write(compressedIndex)

      output.write(int2byteArray(compressedIndex.length))
      output.write(int2byteArray(VERSION_NUMBER))
      output.close()
    }

    private def currentOffset(): Long = index.lastOption.map(_._2).getOrElse(0)
  }
}
