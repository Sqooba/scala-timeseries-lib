package io.sqooba.oss.timeseries.archive

import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, WritableByteChannel}

import io.sqooba.oss.timeseries.archive.MultiSeriesBlock.marshaller
import io.sqooba.oss.timeseries.thrift.TMultiSeriesFooter
import io.sqooba.oss.timeseries.utils.{SliceableByteChannel, ThriftMarshaller}
import org.apache.thrift.TException
import org.apache.thrift.protocol.TCompactProtocol

import scala.annotation.tailrec

// See the doc of the MultiSeriesBlock object for the format specification.

/** The MultiSeriesBlock class just lazily wraps a channel that it reads from.
  * This facilitates the reading of the binary format. The block does not
  * perform any reading on the channel unless a method explicitly requires it.
  *
  * @param channel to read from upon method invocation
  */
case class MultiSeriesBlock(channel: SliceableByteChannel) {

  /** @return the thrift encoded metadata and index in the footer of the block.
    *
    * @throws TException if the metadata cannot be decoded
    */
  def readFooter: TMultiSeriesFooter = {
    require(
      channel.readIntFromEnd(0) == MultiSeriesBlock.STS_MAGIC_NUMBER,
      "The binary format is not valid, or is not an STS block."
    )

    marshaller
      .decode(
        channel.readBytesFromEnd(
          2 * Integer.BYTES,
          channel.readIntFromEnd(Integer.BYTES)
        ))
      .get
  }

  /** @return the reference to a GorillaSuperBlock as indexed by the footer of this block. */
  def getSuperBlock(footer: TMultiSeriesFooter, index: Int): GorillaSuperBlock =
    GorillaSuperBlock(
      channel.slice(footer.offsets(index), footer.offsets(index + 1))
    )

  /** @return the reference to a GorillaSuperBlock as indexed by the footer of this block.
    *
    * @throws NoSuchElementException if there are no string-keys indexing this block
    */
  def getSuperBlock(footer: TMultiSeriesFooter, key: String): GorillaSuperBlock =
    getSuperBlock(footer, footer.keys.get(key))
}

/** A MultiSeriesBlock groups multiple GorillaSuperBlocks in an indexed format.
  * All the SuperBlocks are concatenated and there is a footer managed by thrift
  * that contains the index and optionally names/string-keys for each SuperBlock.
  *
  * The start and the end of the blob are marked with the 4-byte magic number
  * 'STS\n': 53 54 53 0a  that stands for "super time-series".
  *
  * GorillaSuperBlock Format
  * +---+-------+-------+ ... +----- -+----------+---+---+
  * | S | super | super |     | super | Thrift   | L | S |
  * | T | block | block |     | block | footer   | F | T |
  * | S | 0     | 1     |     | N     |          |   | S |
  * +---+-------+-------+ ... +-------+----------+---+---+
  *
  * LF: length of Thrift footer, STS: magic number
  */
object MultiSeriesBlock {

  /** Specifies the version number of this binary format */
  val VERSION_NUMBER = 1

  /** Magic number that serves as a format delimiter. */
  val STS_MAGIC_NUMBER: Int = 0x5354530a
  // TODO: Once, scalafmt gets 2.13 support: 0x53_54_53_0a

  private val marshaller = ThriftMarshaller
    .forType[TMultiSeriesFooter](new TCompactProtocol.Factory)
    .get

  /** Construct and write a MutliSeriesBlock to the given byte channel.
    *
    * @param superBlocks a sequence of file channels to read the GorillaSuperBlocks
    *                    from
    * @param names optional names/string-keys to index the GorillaSuperBlocks,
    *              their order must correspond with the order of superBlocks
    * @param output writable byte channel
    *
    * @throws TException if the footer cannot be encoded
    */
  def write(
      superBlocks: Seq[SliceableByteChannel],
      names: Option[Seq[String]],
      output: WritableByteChannel
  ): Unit = {
    output.write(int2ByteBuffer(STS_MAGIC_NUMBER))

    // Write all the super blocks and calculate the offsets. The first offset
    // stems from the magic number. The last offset points to the start of the
    // footer (thanks to scanLeft).
    val offsets =
      superBlocks.scanLeft(Integer.BYTES.toLong) {
        case (offset, input) =>
          offset + transferAllBytes(input, output)
      }

    val metadata = TMultiSeriesFooter(
      VERSION_NUMBER,
      offsets,
      names.map(_.zipWithIndex.toMap)
    )

    val metaBytes = marshaller.encode(metadata).get
    output.write(ByteBuffer.wrap(metaBytes))

    output.write(int2ByteBuffer(metaBytes.length))
    output.write(int2ByteBuffer(STS_MAGIC_NUMBER))
    output.close()
  }

  private val BUFFER_SIZE = 8192

  /** Writes entire input channel to the given output channel.
    * @note This may be less efficient than FileChannel.transferTo
    * @note This was adapted from Google Guava ByteStreams:
    *       github.com/google/guava/blob/master/guava/src/com/google/common/io/ByteStreams.java#L128
    */
  private def transferAllBytes(input: ReadableByteChannel, output: WritableByteChannel): Long = {
    val buffer = ByteBuffer.allocateDirect(BUFFER_SIZE)
    var total  = 0

    while (input.read(buffer) != -1) {
      buffer.flip()
      while (buffer.hasRemaining) {
        total += output.write(buffer)
      }
      buffer.clear()
    }
    total
  }

}
