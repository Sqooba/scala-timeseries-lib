package io.sqooba.oss.timeseries.archive

import java.io.OutputStream
import java.nio.channels.SeekableByteChannel

import io.sqooba.oss.timeseries.immutable.TSEntry
import io.sqooba.oss.timeseries.thrift.{TBlockType, TSampledBlockType, TSuperBlockMetadata}
import io.sqooba.thrift.ThriftMarshaller
import org.apache.thrift.protocol.TCompactProtocol

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** A GorillaSuperBlock is a binary format for storing a long
  * [[io.sqooba.oss.timeseries.TimeSeries]] composed of many GorillaBlocks. It
  * uses a sequential layout with an index to permit quick look-up of individual
  * blocks by timestamp.
  *
  * GorillaSuperBlock Format
  * +-------+-------+ ... +----- -+-------+---+----------+---+
  * | block | block |     | block | Index | L | Thrift   | L |
  * | 1     | 2     |     | N     |       | I | metadata | T |
  * +-------+-------+ ... +-------+-------+---+----------+---+
  *
  * LT: length of Thrift block, LI: length of index
  *
  * The Gorilla blocks are lined up one after another, ordered by timestamp.
  * They all have different lengths that can be recovered from the offsets
  * stored in the index.
  *
  * At the end of the stream there is a footer composed of an additional Gorilla
  * array containing the index and a block of thrift managed metadata. The index
  * maps the start timestamp of each GorillaBlock to its offset from the
  * beginning of the blob (in bytes). The last entry in the index points to the
  * start of the footer.
  *
  * The lengths of the index and of the thrift block are written as 4 byte
  * integers just after the respective blocks. In that manner a user can first
  * read the footer, decode the index and then do fast look-ups for blocks by
  * timestamp.
  */
object GorillaSuperBlock {

  /**
    * Specifies the version number of this binary format
    */
  val VERSION_NUMBER: Int = 1

  private[archive] val marshaller = ThriftMarshaller
    .forType[TSuperBlockMetadata](new TCompactProtocol.Factory)
    .get

  /**
    * Writes the provided TSEntries of Gorilla blocks to the provided output stream
    * according to the GorillaSuperBlock format.
    *
    * @param buckets well-formed stream of entries containing Gorilla blocks
    * @param output the stream to write to
    */
  def write[B <: GorillaBlock](buckets: Seq[TSEntry[B]], output: OutputStream): Unit =
    buckets.foldLeft(new GorillaSuperBlock.Writer[B](output))(_ += _).close()

  /** Read the metadata of a GorillaSuperBlock from a seekable byte channel.
    * This also checks whether the data from the channel has the correct version.
    *
    * @param channel a seekable byte channel
    * @return the metadata as specified by the thrift definition and its length or Failure
    */
  def readMetadata(channel: SeekableByteChannel): Try[(TSuperBlockMetadata, Int)] = {
    val thriftLength = readIntFromEnd(channel, 0)

    marshaller
      .decode(readBytesFromEnd(channel, Integer.BYTES, thriftLength))
      .map(metadata => {
        require(metadata.version == VERSION_NUMBER, "The binary input's version is not compatible.")
        (metadata, thriftLength)
      })
  }

  /**
    * Read and decompress the index of a given seekable channel. The index must be
    * stored as a footer like it is described by GorillaSuperBlock.
    * @note This assumes that the data from the channel has the correct version.
    *
    * @param channel a seekable byte channel
    * @param thriftBlockLength the length of the thrift block in the footer
    * @return the index as a sorted map of (timestamp -> byte-offset) tuples
    */
  def readIndex(channel: SeekableByteChannel, thriftBlockLength: Int): SortedMap[Long, Long] = {
    val indexBlockEnd = Integer.BYTES + thriftBlockLength

    GorillaArray.decompressTimestampTuples(
      readBytesFromEnd(
        channel,
        indexBlockEnd + Integer.BYTES,
        readIntFromEnd(channel, indexBlockEnd)
      )
    )
  }

  /**
    * Read (but don't decompress) a GorillaBlock from the given seekable channel
    * that contains data in the GorillaSuperBlock format.
    * @note This assumes that the data from the channel has the correct version.
    *
    * @param channel a seekable byte channel
    * @param offset the start of the block as given by the index in the footer
    * @param length the length of the block in bytes
    * @param metadata of the SuperBlock that has information needed for deserialization
    * @return a gorilla encoded block of a timeseries
    */
  def readBlock(
      channel: SeekableByteChannel,
      offset: Long,
      length: Int,
      metadata: TSuperBlockMetadata
  ): GorillaBlock = {
    val bytes = readBytes(channel, offset, length)

    metadata.blockType match {
      case TBlockType.Sample(s) => GorillaBlock.fromSampled(bytes, s.sampleRate)
      case _                    => GorillaBlock.fromTupleSerialized(bytes)
    }
  }

  /**
    * Read (but don't decompress) all GorillaBlocks from the given seekable channel
    * that contains data in the GorillaSuperBlock format.
    * @note This assumes that the data from the channel has the correct version.
    *
    * @param channel a seekable byte channel
    * @param metadata of the SuperBlock that has information needed for deserialization
    * @param metaLength length of the serialized metadata block
    * @return a sequence of gorilla encoded blocks of a timeseries
    */
  def readAllBlocks(
      channel: SeekableByteChannel,
      metadata: TSuperBlockMetadata,
      metaLength: Int
  ): Seq[TSEntry[GorillaBlock]] =
    readIndex(channel, metaLength).toSeq
      .sliding(2)
      .map {
        case (ts, offset) :: (nextTs, nextOffset) :: _ =>
          TSEntry(
            ts,
            readBlock(channel, offset, (nextOffset - offset).toInt, metadata),
            nextTs - ts
          )
      }
      .to(LazyList)

  /** Read all of the information of an entire GorillaSuperBlock. This uses the
    * more granular methods of this object internally.
    *
    * @param channel a seekable byte channel of the file
    * @return a sequence of gorilla blocks
    */
  def readAll(channel: SeekableByteChannel): Seq[TSEntry[GorillaBlock]] =
    readMetadata(channel).map {
      case (metadata, metaLength) => readAllBlocks(channel, metadata, metaLength)
    }.get

  class Writer[B <: GorillaBlock](output: OutputStream) extends mutable.Growable[TSEntry[B]] with AutoCloseable {

    // A map of (timestamps -> offset of encoded block in output)
    private var index: SortedMap[Long, Long]          = SortedMap.empty[Long, Long]
    private var metadata: Option[TSuperBlockMetadata] = None

    private var lastLength       = 0L
    private var lastDefinedUntil = 0L
    private var resultCalled     = false

    override def addOne(entry: TSEntry[B]): Writer.this.type = {
      val TSEntry(ts, gorillaBlock, _) = entry

      if (metadata.isEmpty) {
        metadata = Some(
          TSuperBlockMetadata(VERSION_NUMBER, blockType(gorillaBlock))
        )
      }

      val blockBytes = gorillaBlock.serialize
      output.write(blockBytes)

      index += (ts -> (currentOffset + lastLength))
      lastLength = blockBytes.length
      lastDefinedUntil = entry.definedUntil

      this
    }

    override def clear(): Unit = {
      index = SortedMap.empty[Long, Long]
      lastLength = 0
      lastDefinedUntil = 0
      resultCalled = false
    }

    override def close(): Unit = {
      if (index.isEmpty || metadata.isEmpty) {
        throw new IllegalStateException("Cannot write a GorillaSuperBlock of zero GorillaBlocks.")
      } else if (resultCalled) {
        throw new IllegalStateException("Cannot call result more than once, unless the builder was cleared.")
      }

      resultCalled = true

      // Add a last entry to the index. It points to the end of the last block
      // i.e. serving as an end marker.
      index += (lastDefinedUntil -> (currentOffset + lastLength))

      // write the compressed index and its length
      val compressedIndex = GorillaArray.compressTimestampTuples(index)
      output.write(compressedIndex)
      output.write(int2byteArray(compressedIndex.length))

      // write the metadata and its length
      val metaBlock = marshaller.encode(metadata.get).get
      output.write(metaBlock)
      output.write(int2byteArray(metaBlock.length))

      output.close()
    }

    private def currentOffset(): Long = index.lastOption.map(_._2).getOrElse(0)

    private def blockType(gorillaBlock: GorillaBlock): TBlockType =
      gorillaBlock match {
        case SampledGorillaBlock(_, rate) => TBlockType.Sample(TSampledBlockType(rate))
        case _                            => TBlockType.Tuple()
      }
  }
}
